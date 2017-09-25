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
import com.netflix.fenzo.sla.ResAllocsUtil;
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
    private final ResAllocs emptyBucketGuarantees;
    private ResAllocs bucketGuarantees;
    private ResAllocs effectiveUsage;

    private final LinkedHashMap<String, QueuableTask> queuedTasks;
    private final LinkedHashMap<String, QueuableTask> launchedTasks;
    // Assigned tasks is a temporary holder for tasks being assigned resources during scheduling
    // iteration. These tasks are duplicate entries of tasks in queuedTasks, which cannot be removed from queuedTasks
    // collection in order to keep the iterator on queuedTasks consistent throughout the scheduling iteration. Remember
    // that scheduler's taskTracker will trigger call into assignTask() during the scheduling iteration.
    private final LinkedHashMap<String, QueuableTask> assignedTasks;
    private Iterator<Map.Entry<String, QueuableTask>> iterator = null;
    private ResAllocs tierResources;
    private final BiFunction<Integer, String, Double> allocsShareGetter;
    private final ResUsage tierUsage;

    QueueBucket(int tierNumber, String name, ResUsage tierUsage, BiFunction<Integer, String, Double> allocsShareGetter) {
        this.tierNumber = tierNumber;
        this.name = name;
        this.tierUsage = tierUsage;
        totals = new ResUsage();
        this.emptyBucketGuarantees = ResAllocsUtil.emptyOf(name);
        bucketGuarantees = emptyBucketGuarantees;
        queuedTasks = new LinkedHashMap<>();
        launchedTasks = new LinkedHashMap<>();
        assignedTasks = new LinkedHashMap<>();
        this.allocsShareGetter = allocsShareGetter == null ?
                (integer, s) -> 1.0 :
                allocsShareGetter;
    }

    void setBucketGuarantees(ResAllocs bucketGuarantees) {
        this.bucketGuarantees = bucketGuarantees == null ? emptyBucketGuarantees : bucketGuarantees;
        updateEffectiveUsage();
    }

    ResAllocs getBucketGuarantees() {
        return bucketGuarantees;
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
    public Assignable<QueuableTask> nextTaskToLaunch() throws TaskQueueException {
        if (iterator == null) {
            iterator = queuedTasks.entrySet().iterator();
            if (!assignedTasks.isEmpty())
                throw new TaskQueueException(assignedTasks.size() + " tasks still assigned but not launched");
        }
        while (iterator.hasNext()) {
            final Map.Entry<String, QueuableTask> nextTask = iterator.next();
            if (nextTask.getValue().getReadyAt() <= System.currentTimeMillis())
                return Assignable.success(nextTask.getValue());
        }
        return null;
    }

    @Override
    public void assignTask(QueuableTask t) throws TaskQueueException {
        if (iterator == null)
            throw new TaskQueueException(new IllegalStateException("assign called on task " + t.getId() + " while not iterating over tasks"));
        if (queuedTasks.get(t.getId()) == null)
            throw new TaskQueueException("Task not in queue for assigning, id=" + t.getId());
        if (assignedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Task already assigned, id=" + t.getId());
        if (launchedTasks.get(t.getId()) != null)
            throw new TaskQueueException("Task already launched, id=" + t.getId());
        assignedTasks.put(t.getId(), t);
        addUsage(t);
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
            addUsage(t);
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
                removeUsage(removed);
        }
        return removed;
    }

    private void addUsage(QueuableTask t) {
        totals.addUsage(t);
        updateEffectiveUsage();
    }

    private void removeUsage(QueuableTask removed) {
        totals.remUsage(removed);
        updateEffectiveUsage();
    }

    private void updateEffectiveUsage() {
        effectiveUsage = ResAllocsUtil.ceilingOf(totals.getResAllocsWrapper(), bucketGuarantees);
    }

    @Override
    public double getDominantUsageShare() {
        // If total tier capacity is not available, use current tier allocation as a base for share computation.
        ResAllocs total = tierResources == null ? tierUsage.getResAllocsWrapper() : tierResources;
        return totals.getDominantResUsageFrom(total) /
                Math.max(TierSla.eps / 10.0, allocsShareGetter.apply(tierNumber, name));
    }

    @Override
    public void setTaskReadyTime(String taskId, QAttributes qAttributes, long when) throws TaskQueueException {
        if (iterator != null)
            throw new TaskQueueException("Must reset before setting task ready time");
        final QueuableTask task = queuedTasks.get(taskId);
        if (task != null)
            task.safeSetReadyAt(when);
    }

    public boolean hasGuaranteedCapacityFor(QueuableTask task) {
        // Check first if we are already above the limit
        if (!ResAllocsUtil.isBounded(totals.getResAllocsWrapper(), bucketGuarantees)) {
            return false;
        }

        // We have some remaining guaranteed resources. Now check if they are enough for our task.
        ResAllocs summed = ResAllocsUtil.add(totals.getResAllocsWrapper(), task);
        return ResAllocsUtil.isBounded(summed, bucketGuarantees);
    }

    public ResAllocs getEffectiveUsage() {
        return effectiveUsage;
    }

    public String getBucketCapacityAsString() {
        StringBuilder sb = new StringBuilder();
        if (bucketGuarantees != null) {
            sb.append("Bucket ").append(name).append(" Total Capacity: ").append(bucketGuarantees.getAsString());
        }
        if (effectiveUsage != null) {
            sb.append("\nBucket ").append(name).append(" Used Capacity: ").append(effectiveUsage.getAsString());
        }
        return sb.toString();
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
        this.tierResources = ResAllocsUtil.toResAllocs("tier", totalResourcesMap);
    }

    public void setTotalResources(ResAllocs tierResources) {
        this.tierResources = tierResources;
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
