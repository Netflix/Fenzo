/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.queues.*;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;

/**
 * A queue bucket is a collection of tasks in one bucket. Generally, all tasks in the bucket are associated
 * with a single entity for scheduling purposes such as capacity guarantees.
 */
class QueueBucket implements UsageTrackedQueue {
    private static final Logger logger = LoggerFactory.getLogger(QueueBucket.class);
    private final int tierNumber;
    private final String name;
    private final ResUsage totals;
    private final LinkedHashMap<String, QueuableTask> queuedTasks;
    private final LinkedHashMap<String, QueuableTask> launchedTasks;
    // Assigned tasks is a temporary holder for tasks being assigned resources during scheduling
    // iteration. These tasks are duplicate entries of tasks in queuedTasks, which cannot be removed from queuedTasks
    // collection in order to keep the iterator on queuedTasks consistent throughout the scheduling iteration. Remember
    // that scheduler's taskTracker will trigger call into assignTask() during the scheduling iteration.
    private final LinkedHashMap<String, QueuableTask> assignedTasks;
    private Iterator<Map.Entry<String, QueuableTask>> iterator = null;

    private Map<VMResource, Double> totalResourcesMap = Collections.emptyMap();
    private ResAllocs totalResAllocs;

    private final BiFunction<Integer, String, Double> allocsShareGetter;
    private boolean belowGuaranteedConsumption;

    QueueBucket(int tierNumber, String name, BiFunction<Integer, String, Double> allocsShareGetter) {
        this.tierNumber = tierNumber;
        this.name = name;
        totals = new ResUsage(name);
        totalResAllocs = ResAllocsUtil.emptyOf(name);
        queuedTasks = new LinkedHashMap<>();
        launchedTasks = new LinkedHashMap<>();
        assignedTasks = new LinkedHashMap<>();
        this.allocsShareGetter = allocsShareGetter == null ?
                (integer, s) -> 1.0 :
                allocsShareGetter;
    }

    @Override
    public void queueTask(QueuableTask t) throws TaskQueueException {
        if (iterator != null)
            throw new ConcurrentModificationException("Must reset before queuing tasks");
        if (queuedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Duplicate task not allowed, task with id " + t.getId());
        if (launchedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Task already launched, can't queue, id=" + t.getId());
        queuedTasks.put(t.getId(), t);
    }

    @Override
    public QueuableTask nextTaskToLaunch() throws TaskQueueException {
        if (iterator == null) {
            iterator = queuedTasks.entrySet().iterator();
            if (!assignedTasks.isEmpty())
                throw new TaskQueueException(assignedTasks.size() + " tasks still assigned but not launched");
        }
        if (iterator.hasNext())
            return iterator.next().getValue();
        return null;
    }

    @Override
    public void assignTask(QueuableTask t) throws TaskQueueException {
        if (iterator == null)
            throw new TaskQueueException(new IllegalStateException("assign called while not iterating over tasks"));
        if (queuedTasks.get(t.getId()) == null)
            throw new TaskQueueException("Task not in queue for assigning, id=" + t.getId());
        if (assignedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Task already assigned, id=" + t.getId());
        if (launchedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Task already launched, id=" + t.getId());
        assignedTasks.put(t.getId(), t);
        totals.addUsage(t);
    }

    @Override
    public boolean launchTask(QueuableTask t) throws TaskQueueException {
        if (iterator != null)
            throw new ConcurrentModificationException("Must reset before launching tasks");
        if (launchedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Task already launched, id=" + t.getId());
        queuedTasks.remove(t.getId());
        final QueuableTask removed = assignedTasks.remove(t.getId());
        launchedTasks.put(t.getId(), t);
        if (removed == null) { // queueTask usage only if it was not assigned, happens when initializing tasks that were running previously
            totals.addUsage(t);
            return true;
        }
        return false;
    }

    @Override
    public QueuableTask removeTask(String id, QAttributes qAttributes) throws TaskQueueException {
        if (iterator != null)
            throw new TaskQueueException("Must reset before removing tasks");
        QueuableTask removed = queuedTasks.remove(id);
        if (removed == null) {
            removed = assignedTasks.remove(id);
            if (removed == null)
                removed = launchedTasks.remove(id);
            if (removed != null)
                totals.remUsage(removed);
        }
        return removed;
    }

    @Override
    public double getDominantUsageShare() {
        return totals.getDominantResUsageFrom(totalResourcesMap) /
                Math.max(TierSla.eps / 10.0, allocsShareGetter.apply(tierNumber, name));
    }

    public ResAllocs getUsedOrGuaranteed() {
        return ResAllocsUtil.ceilingOf(totalResAllocs, totals.asResAllocs());
    }

    public boolean isBelowGuaranteedConsumption() {
        return ResAllocsUtil.isLess(totals.asResAllocs(), totalResAllocs);
    }

    @Override
    public void reset() {
        iterator = null;
    }

    @Override
    public Map<TaskQueue.TaskState, Collection<QueuableTask>> getAllTasks() throws TaskQueueException {
        if (iterator != null)
            throw new TaskQueueException("Must reset before getting list of tasks");
        Map<TaskQueue.TaskState, Collection<QueuableTask>> result = new HashMap<>();
        result.put(TaskQueue.TaskState.QUEUED, Collections.unmodifiableCollection(queuedTasks.values()));
        result.put(TaskQueue.TaskState.LAUNCHED, Collections.unmodifiableCollection(launchedTasks.values()));
        return result;
    }

    @Override
    public void setTotalResources(Map<VMResource, Double> totalResourcesMap) {
        this.totalResourcesMap = totalResourcesMap;
        this.totalResAllocs = new ResAllocsBuilder(name)
                .withCores(totalResourcesMap.getOrDefault(VMResource.CPU, 0.0))
                .withMemory(totalResourcesMap.getOrDefault(VMResource.Memory, 0.0))
                .withNetworkMbps(totalResourcesMap.getOrDefault(VMResource.Network, 0.0))
                .withDisk(totalResourcesMap.getOrDefault(VMResource.Disk, 0.0))
                .build();
    }

    int size() {
        return queuedTasks.size() + launchedTasks.size(); // don't queueTask assignedTasks.size(), they are duplicate of queuedTasks
    }

    int getTierNumber() {
        return tierNumber;
    }

    String getName() {
        return name;
    }
}
