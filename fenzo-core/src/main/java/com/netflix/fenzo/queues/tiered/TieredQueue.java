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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;

/**
 * A tiered queuing system where queues are arranged in multiple tiers and then among multiple buckets within each tier.
 * Tiers represent coarse grain priority, in which higher tier's queues are considered for resource assignment
 * before any of the lower tiers' queues are considered. Within a tier, multiple queues are considered for resource
 * assignment such that their dominant resource usage shares are similar. For example, a queue bucket using 60% of the
 * total memory in use is said to be similar in usage to another bucket using 60% of the total CPUs in use, even if the
 * latter's memory usage is, say, only 10%. The tiers are numbered {@code 0} to {@code N-1} for {@code N} tiers, with
 * {@code 0} being the highest priority level.
 */
public class TieredQueue implements InternalTaskQueue {

    private static final Logger logger = LoggerFactory.getLogger(TieredQueue.class);
    private final List<Tier> tiers;
    private Iterator<Tier> iterator = null;
    private Tier currTier = null;
    private final BlockingQueue<QueuableTask> tasksToQueue;
    private final BlockingQueue<TieredQueueSlas> slasQueue;
    private final TierSlas tierSlas = new TierSlas();
    private final BiFunction<Integer, String, Double> allocsShareGetter = tierSlas::getBucketAllocation;

    /**
     * Construct a tiered queue system with the given number of tiers.
     * @param numTiers The number of tiers.
     */
    public TieredQueue(int numTiers) {
        tiers = new ArrayList<>(numTiers);
        for ( int i=0; i<numTiers; i++ )
            tiers.add(new Tier(i, allocsShareGetter));
        tasksToQueue = new LinkedBlockingQueue<>();
        slasQueue = new LinkedBlockingQueue<>();
    }

    public int getNumTiers() {
        return tiers.size();
    }

    @Override
    public void queueTask(QueuableTask task) {
        tasksToQueue.offer(task);
    }

    @Override
    public void setSla(TaskQueueSla sla) throws IllegalArgumentException {
        if (sla != null && !(sla instanceof TieredQueueSlas)) {
            throw new IllegalArgumentException("Queue SLA must be an instance of " + TieredQueueSlas.class.getName() +
                    ", can't accept " + sla.getClass().getName());
        }
        slasQueue.offer(sla == null? new TieredQueueSlas(Collections.emptyMap(), Collections.emptyMap()) : (TieredQueueSlas)sla);
    }

    private void setSlaInternal() {
        if (slasQueue.peek() != null) {
            List<TieredQueueSlas> slas = new ArrayList<>();
            slasQueue.drainTo(slas);
            tierSlas.setAllocations(slas.get(slas.size()-1)); // set the last one

            tiers.forEach(tier -> tier.setTierSla(tierSlas.getTierSla(tier.getTierNumber())));
        }
    }

    private void addInternal(QueuableTask task) throws TaskQueueException {
        final int tierNumber = task.getQAttributes().getTierNumber();
        if ( tierNumber >= tiers.size() )
            throw new InvalidTierNumberException(tierNumber, tiers.size());
        tiers.get(tierNumber).queueTask(task);
    }

    /**
     * This implementation dynamically picks the next task to consider for resource assignment based on tiers and then
     * based on current dominant resource usage. The usage is updated with each resource assignment during the
     * scheduling iteration, in addition to updating with all running jobs from before.
     * @return The next task to assign resources to, a task with assignment failure if the task cannot be scheduled due to some
     *         internal constraints (for example exceeds allowed resource usage for a queue). Returns {@code null} if none
     *         remain for consideration.
     * @throws TaskQueueException if there is an unknown error getting the next task to launch from any of the tiers or
     * queue buckets.
     */
    @Override
    public Assignable<QueuableTask> next() throws TaskQueueException {
        if (iterator == null) {
            iterator = tiers.iterator();
            currTier = null;
        }
        if (currTier != null) {
            final Assignable<QueuableTask> taskOrFailure = currTier.nextTaskToLaunch();
            if (taskOrFailure != null)
                return taskOrFailure;
            currTier = null; // currTier all done
        }
        while (currTier == null && iterator.hasNext()) {
            if(iterator.hasNext()) {
                currTier = iterator.next();
                final Assignable<QueuableTask> taskOrFailure = currTier.nextTaskToLaunch();
                if (taskOrFailure != null)
                    return taskOrFailure;
                else
                    currTier = null; // currTier is done
            }
        }
        return null;
    }

    @Override
    public boolean reset() throws TaskQueueMultiException {
        setSlaInternal();
        iterator = null;
        boolean queueChanged = false;
        List<Exception> exceptions = new LinkedList<>();
        if (tasksToQueue.peek() != null) {
            final List<QueuableTask> toQueue = new LinkedList<>();
            tasksToQueue.drainTo(toQueue);
            if (!toQueue.isEmpty()) {
                for (QueuableTask t : toQueue)
                    try {
                        addInternal(t);
                        queueChanged = true;
                    } catch (TaskQueueException e) {
                        exceptions.add(e);
                    }
            }
        }
        if (!exceptions.isEmpty())
            throw new TaskQueueMultiException(exceptions);
        return queueChanged;
    }

    /**
     * This method provides a bridge to the usage tracked queues contained within the tiered queues implementation.
     * @return Implementation for {@link UsageTrackedQueue} to account for all the queues within this tiered queue
     * implementation. This implementation focuses on usage tracking only and therefore does not allow invoking
     * {@link UsageTrackedQueue#nextTaskToLaunch()} and {@link UsageTrackedQueue#getAllTasks()}.
     */
    @Override
    public UsageTrackedQueue getUsageTracker() {
        return new UsageTrackedQueue() {
            @Override
            public void queueTask(QueuableTask t) throws TaskQueueException {
                tiers.get(t.getQAttributes().getTierNumber()).queueTask(t);
            }

            @Override
            public Assignable<QueuableTask> nextTaskToLaunch() {
                return null;
            }

            @Override
            public void assignTask(QueuableTask t) throws TaskQueueException {
                tiers.get(t.getQAttributes().getTierNumber()).assignTask(t);
            }

            @Override
            public boolean launchTask(QueuableTask t) throws TaskQueueException {
                return tiers.get(t.getQAttributes().getTierNumber()).launchTask(t);
            }

            @Override
            public QueuableTask removeTask(String id, QAttributes qAttributes) throws TaskQueueException {
                return tiers.get(qAttributes.getTierNumber()).removeTask(id, qAttributes);
            }

            @Override
            public double getDominantUsageShare() {
                return 0.0;
            }

            @Override
            public void setTaskReadyTime(String taskId, QAttributes qAttributes, long when) throws TaskQueueException {
                tiers.get(qAttributes.getTierNumber()).setTaskReadyTime(taskId, qAttributes, when);
            }

            @Override
            public void reset() {
                for(Tier tb: tiers)
                    tb.reset();
            }

            @Override
            public Map<TaskState, Collection<QueuableTask>> getAllTasks() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setTotalResources(Map<VMResource, Double> totalResourcesMap) {
                for (Tier t: tiers)
                    t.setTotalResources(totalResourcesMap);
            }
        };
    }

    @Override
    public Map<TaskState, Collection<QueuableTask>> getAllTasks() {
        Map<TaskState, Collection<QueuableTask>> result = new HashMap<>();
        for (Tier tb: tiers) {
            try {
                final Map<TaskState, Collection<QueuableTask>> allTasks = tb.getAllTasks();
                if (!allTasks.isEmpty()) {
                    for (TaskState s: TaskState.values()) {
                        final Collection<QueuableTask> t = allTasks.get(s);
                        if (t != null && !t.isEmpty()) {
                            Collection<QueuableTask> st = result.get(s);
                            if (st == null) {
                                st = new LinkedList<>();
                                result.put(s, st);
                            }
                            st.addAll(t);
                        }
                    }
                }
            } catch (TaskQueueException e) {
                logger.error("Unexpected: " + e.getMessage(), e);
            }
        }
        return result;
    }
}
