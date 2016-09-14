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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

class TierBuckets implements UsageTrackedQueue {

    private final int tierNumber;
    private final ResUsage totals;
    private final SortedBuckets sortedBuckets;

    TierBuckets(int tierNumber) {
        totals = new ResUsage();
        this.tierNumber = tierNumber;
        // TODO: need to consider the impact of this comparator to any others we may want, like simple round robin.
        // Use DRF sorting. Except, note that it is undefined when two entities compare to 0 (equal values) which
        // one gets ahead of the other.
        sortedBuckets = new SortedBuckets(totals);
    }

    private QueueBucket getOrCreateBucket(QueuableTask t) {
        if (t == null)
            throw new NullPointerException();
        final String bucketName = t.getQAttributes().getBucketName();
        QueueBucket bucket = sortedBuckets.get(bucketName);
        if (bucket == null) {
            bucket = new QueueBucket(tierNumber, bucketName);
            sortedBuckets.add(bucket);
        }
        return bucket;
    }

    @Override
    public void queueTask(QueuableTask t) throws TaskQueueException {
        getOrCreateBucket(t).queueTask(t);
    }

    public QueuableTask nextTaskToLaunch() throws TaskQueueException {
        for (QueueBucket bucket: sortedBuckets.getSortedList()) {
            final QueuableTask task = bucket.nextTaskToLaunch();
            if (task != null) {
                return task;
            }
        }
        return null;
    }

    @Override
    public void assignTask(QueuableTask t) throws TaskQueueException {
        // assigning the task changes resource usage and therefore, sorting order must be updated.
        // We do this by removing the bucket from sortedBuckets, assigning the task in the bucket,
        // then adding the bucket back into the sortedBuckets. It will then fall into its right new place.
        // This operation therefore takes time complexity of O(log N).
        final QueueBucket bucket = sortedBuckets.remove(t.getQAttributes().getBucketName());
        if (bucket == null)
            throw new TaskQueueException("Invalid to not find bucket to assign task id=" + t.getId());
        try {
            bucket.assignTask(t);
            totals.addUsage(t);
        }
        finally {
            sortedBuckets.add(bucket);
        }
    }

    @Override
    public boolean launchTask(QueuableTask t) throws TaskQueueException {
        // launching the task changes the resource usage and therefore sorting order must be updated.
        // We do this by removing the bucket from the sortedBuckets, launching the task in the bucket,
        // then adding the bucket back into the sortedBuckets. It will then fall into its right new place.
        // This operation therefore takes time complexity of O(log N).
        final String bucketName = t.getQAttributes().getBucketName();
        QueueBucket bucket = sortedBuckets.remove(bucketName);
        if (bucket == null) {
            bucket = new QueueBucket(tierNumber, bucketName);
        }
        try {
            if (bucket.launchTask(t)) {
                totals.addUsage(t);
                return true;
            }
        }
        finally {
            sortedBuckets.add(bucket);
        }
        return false;
    }

    @Override
    public QueuableTask removeTask(String id, QAttributes qAttributes) throws TaskQueueException {
        // removing a task can change the resource usage and therefore the sorting order of queues. So, we take the
        // same approach as in launchTask() above - remove the bucket and readd to keep sorting order updated.
        final QueueBucket bucket = sortedBuckets.remove(qAttributes.getBucketName());
        if (bucket == null)
            return null;
        final QueuableTask removed;
        try {
            removed = bucket.removeTask(id, qAttributes);
            if (removed != null) {
                totals.remUsage(removed);
            }
        }
        finally {
            if (bucket.size() > 0)
                sortedBuckets.add(bucket);
        }
        return removed;
    }

    @Override
    public double getDominantUsageShare(ResUsage parentUsage) {
        return 0.0; // undefined for a tier
    }

    @Override
    public void reset() throws TaskQueueException {
        for (QueueBucket bucket: sortedBuckets.getSortedList()) {
            bucket.reset();
        }
    }

    public Map<TaskQueue.State, Collection<QueuableTask>> getAllTasks() throws TaskQueueException {
        Map<TaskQueue.State, Collection<QueuableTask>> result = new HashMap<>();
        for (QueueBucket bucket: sortedBuckets.getSortedList()) {
            final Map<TaskQueue.State, Collection<QueuableTask>> allTasks = bucket.getAllTasks();
            if (!allTasks.isEmpty()) {
                for (TaskQueue.State s: TaskQueue.State.values()) {
                    final Collection<QueuableTask> q = allTasks.get(s);
                    if (q != null && !q.isEmpty()) {
                        Collection<QueuableTask> resQ = result.get(s);
                        if (resQ == null) {
                            resQ = new LinkedList<>();
                            result.put(s, resQ);
                        }
                        resQ.addAll(q);
                    }
                }
            }
        }
        return result;
    }
}
