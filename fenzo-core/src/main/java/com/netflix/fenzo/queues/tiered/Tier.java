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

/**
 * This class represents a tier of the multi-tiered queue that {@link TieredQueue} represents. The tier holds one or
 * more buckets for queues, {@link QueueBucket} and maintains them in an order defined by the dynamic value for
 * dominant resource usage via {@link SortedBuckets}. The values are dynamically updated via the implementation of
 * {@link UsageTrackedQueue} this class provides.
 */
class Tier implements UsageTrackedQueue {

    private static final Logger logger = LoggerFactory.getLogger(Tier.class);
    private final int tierNumber;
    private final ResUsage totals;
    private final SortedBuckets sortedBuckets;
    private Map<VMResource, Double> currTotalResourcesMap = new HashMap<>();

    Tier(int tierNumber) {
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

    @Override
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
        if (logger.isDebugEnabled())
            logger.debug("Adding " + t.getId() + ": to ordered buckets: " + getSortedListString());
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

    private void verifySortedBuckets() throws TaskQueueException {
        if (sortedBuckets.getSortedList().isEmpty())
            return;
        List<QueueBucket> list = new ArrayList<>(sortedBuckets.getSortedList());
        if (list.size() > 1) {
            QueueBucket prev = list.get(0);
            for (int i=1; i<list.size(); i++) {
                if (list.get(i).getDominantUsageShare() < prev.getDominantUsageShare()) {
                    final String msg = "Incorrect sorting order : " + getSortedListString();
                    throw new TaskQueueException(msg);
                }
                prev = list.get(i);
            }
        }
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
    public double getDominantUsageShare() {
        return 0.0; // undefined for a tier
    }

    @Override
    public void reset() {
        if (logger.isDebugEnabled()) {
            try {
                verifySortedBuckets();
            } catch (TaskQueueException e) {
                logger.error(e.getMessage());
            }
        }
        for (QueueBucket bucket: sortedBuckets.getSortedList()) {
            bucket.reset();
        }
    }

    private String getSortedListString() {
        StringBuilder b = new StringBuilder("Tier " + tierNumber + " sortedBs: [");
        for (QueueBucket bucket: sortedBuckets.getSortedList()) {
            b.append(bucket.getName()).append(" (").append(bucket.getDominantUsageShare()).append("), ");
        }
        b.append("]");
        return b.toString();
    }

    @Override
    public void setTotalResources(Map<VMResource, Double> totalResourcesMap) {
        if (totalResMapChanged(currTotalResourcesMap, totalResourcesMap)) {
            currTotalResourcesMap.clear();
            currTotalResourcesMap.putAll(totalResourcesMap);
            for (QueueBucket b: sortedBuckets.getSortedList()) {
                b.setTotalResources(totalResourcesMap);
            }
            logger.info("Re-sorting buckets in tier " + tierNumber + " after totals changed");
            sortedBuckets.resort();
        }
    }

    private boolean totalResMapChanged(Map<VMResource, Double> currTotalResourcesMap, Map<VMResource, Double> totalResourcesMap) {
        if (currTotalResourcesMap.size() != totalResourcesMap.size())
            return true;
        Set<VMResource> curr = new HashSet<>(currTotalResourcesMap.keySet());
        for (VMResource r: totalResourcesMap.keySet()) {
            final Double c = currTotalResourcesMap.get(r);
            final Double n = totalResourcesMap.get(r);
            if ((c == null && n != null) || (c != null && n == null) || (n != null &&!n.equals(c)))
                return true;
            curr.remove(r);
        }
        return !curr.isEmpty();
    }

    @Override
    public Map<TaskQueue.TaskState, Collection<QueuableTask>> getAllTasks() throws TaskQueueException {
        Map<TaskQueue.TaskState, Collection<QueuableTask>> result = new HashMap<>();
        for (QueueBucket bucket: sortedBuckets.getSortedList()) {
            final Map<TaskQueue.TaskState, Collection<QueuableTask>> allTasks = bucket.getAllTasks();
            if (!allTasks.isEmpty()) {
                for (TaskQueue.TaskState s: TaskQueue.TaskState.values()) {
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
