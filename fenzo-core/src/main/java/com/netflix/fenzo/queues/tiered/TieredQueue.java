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

import com.netflix.fenzo.queues.*;
import com.netflix.fenzo.queues.TaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TieredQueue implements InternalTaskQueue {

    private static final Logger logger = LoggerFactory.getLogger(TieredQueue.class);
    private final List<TierBuckets> tiers;
    private Iterator<TierBuckets> iterator = null;
    private TierBuckets currTier = null;
    private final BlockingQueue<QueuableTask> tasksToAdd;
    private final BlockingQueue<QueuableTask> tasksIdsToRemove;

    public TieredQueue(int numTiers) {
        tiers = new ArrayList<>(numTiers);
        for ( int i=0; i<numTiers; i++ )
            tiers.add(new TierBuckets(i));
        tasksToAdd = new LinkedBlockingQueue<>();
        tasksIdsToRemove = new LinkedBlockingQueue<>();
    }


    @Override
    public void add(QueuableTask task) {
        tasksToAdd.offer(task);
    }

    private void addInternal(QueuableTask task) throws TaskQueueException {
        final int tierNumber = task.getQAttributes().getTierNumber();
        if ( tierNumber >= tiers.size() )
            throw new TaskQueueException("Invalid tier number, must be <" + tiers.size());
        tiers.get(tierNumber).queueTask(task);
    }

    @Override
    public void remove(QueuableTask task) {
        tasksIdsToRemove.offer(task);
    }

    private boolean removeInternal(QueuableTask task) throws TaskQueueException {
        final TierBuckets tierBuckets = tiers.get(task.getQAttributes().getTierNumber());
        return tierBuckets.removeTask(task);
    }

    @Override
    public QueuableTask next() throws TaskQueueException {
        if (iterator == null) {
            iterator = tiers.iterator();
            currTier = null;
        }
        if (currTier != null) {
            final QueuableTask task = currTier.nextTaskToLaunch();
            if (task != null)
                return task;
            currTier = null; // currTier all done
        }
        while (currTier == null && iterator.hasNext()) {
            if(iterator.hasNext()) {
                currTier = iterator.next();
                final QueuableTask task = currTier.nextTaskToLaunch();
                if (task != null)
                    return task;
                else
                    currTier = null; // currTier is done
            }
        }
        return null;
    }

    @Override
    public List<Exception> reset() {
        iterator = null;
        List<Exception> exceptions = new ArrayList<>();
        final List<QueuableTask> toAdd = new ArrayList<>();
        tasksToAdd.drainTo(toAdd);
        if (!toAdd.isEmpty()) {
            for(QueuableTask t: toAdd)
                try {
                    addInternal(t);
                } catch (TaskQueueException e) {
                    exceptions.add(e);
                }
        }
        List<QueuableTask> tasksToRem = new ArrayList<>();
        tasksIdsToRemove.drainTo(tasksToRem);
        if (!tasksToRem.isEmpty()) {
            for (QueuableTask task: tasksToRem)
                try {
                    if (!removeInternal(task))
                        exceptions.add(new TaskQueueException("Task with id " + task.getId() + " not found to unqueueTask"));
                } catch (TaskQueueException e) {
                    exceptions.add(e);
                }
        }
        return exceptions;
    }

    @Override
    public UsageTrackedQueue getUsageTracker() {
        return new UsageTrackedQueue() {
            @Override
            public void queueTask(QueuableTask t) throws TaskQueueException {
                tiers.get(t.getQAttributes().getTierNumber()).queueTask(t);
            }

            @Override
            public QueuableTask nextTaskToLaunch() {
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
            public boolean removeTask(QueuableTask t) throws TaskQueueException {
                return tiers.get(t.getQAttributes().getTierNumber()).removeTask(t);
            }

            @Override
            public double getDominantUsageShare(ResUsage parentUsage) {
                return 0.0;
            }

            @Override
            public void reset() throws TaskQueueException {
                for(TierBuckets tb: tiers)
                    tb.reset();
            }

            @Override
            public Map<TaskQueue.State, Collection<QueuableTask>> getAllTasks() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Map<State, Collection<QueuableTask>> getAllTasks() {
        Map<State, Collection<QueuableTask>> result = new HashMap<>();
        for (TierBuckets tb: tiers) {
            try {
                final Map<State, Collection<QueuableTask>> allTasks = tb.getAllTasks();
                if (!allTasks.isEmpty()) {
                    for (State s: State.values()) {
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
