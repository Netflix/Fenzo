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

import java.util.*;
import java.util.function.BiFunction;

import com.netflix.fenzo.AssignmentFailure;
import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.queues.*;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a tier of the multi-tiered queue that {@link TieredQueue} represents. The tier holds one or
 * more buckets for queues, {@link QueueBucket} and maintains them in an order defined by the dynamic value for
 * dominant resource usage via {@link SortedBuckets}. The values are dynamically updated via the implementation of
 * {@link UsageTrackedQueue} this class provides.
 */
class Tier implements UsageTrackedQueue {

    private static final Logger logger = LoggerFactory.getLogger(Tier.class);
    private final int tierNumber;
    private final String tierName;

    private TierSla tierSla;
    private final ResUsage totals;

    private ResAllocs tierResources = null;
    private ResAllocs effectiveUsedResources;
    private ResAllocs remainingResources = null;
    private final Map<String, ResAllocs> lastEffectiveUsedResources = new HashMap<>();

    private final SortedBuckets sortedBuckets;
    private Map<VMResource, Double> currTotalResourcesMap = new HashMap<>();
    private final BiFunction<Integer, String, Double> allocsShareGetter;

    Tier(int tierNumber, BiFunction<Integer, String, Double> allocsShareGetter) {
        this.tierNumber = tierNumber;
        this.tierName = "tier#" + tierNumber;

        this.totals = new ResUsage();
        this.effectiveUsedResources = ResAllocsUtil.emptyOf(tierName);

        // TODO: need to consider the impact of this comparator to any others we may want, like simple round robin.
        // Use DRF sorting. Except, note that it is undefined when two entities compare to 0 (equal values) which
        // one gets ahead of the other.
        sortedBuckets = new SortedBuckets(totals);
        this.allocsShareGetter = allocsShareGetter;
    }

    void setTierSla(TierSla tierSla) {
        this.tierSla = tierSla;

        if (tierSla == null) {
            sortedBuckets.getSortedList().forEach(bucket -> bucket.setBucketGuarantees(null));
            tierResources = ResAllocsUtil.emptyOf(tierName);
        } else {
            sortedBuckets.getSortedList().forEach(bucket -> bucket.setBucketGuarantees(tierSla.getBucketAllocs(bucket.getName())));

            // Always create a bucket, if there is SLA defined for it for proper accounting
            tierSla.getAllocsMap().keySet().forEach(this::getOrCreateBucket);
            this.tierResources = tierSla.getTierCapacity();
        }

        this.effectiveUsedResources = ResAllocsUtil.emptyOf(tierName);
        this.lastEffectiveUsedResources.clear();
        for (QueueBucket bucket : sortedBuckets.getSortedList()) {
            effectiveUsedResources = ResAllocsUtil.add(effectiveUsedResources, bucket.getEffectiveUsage());
            lastEffectiveUsedResources.put(bucket.getName(), bucket.getEffectiveUsage());
        }

        this.remainingResources = ResAllocsUtil.subtract(tierResources, effectiveUsedResources);

        sortedBuckets.resort();
    }

    private QueueBucket getOrCreateBucket(QueuableTask t) {
        if (t == null)
            throw new NullPointerException();
        return getOrCreateBucket(t.getQAttributes().getBucketName());
    }

    private QueueBucket getOrCreateBucket(String bucketName) {
        QueueBucket bucket = sortedBuckets.get(bucketName);
        if (bucket == null) {
            bucket = new QueueBucket(tierNumber, bucketName, totals, allocsShareGetter);
            sortedBuckets.add(bucket);
            bucket.setBucketGuarantees(tierSla == null ? null : tierSla.getBucketAllocs(bucketName));
        }
        return bucket;
    }

    public int getTierNumber() {
        return tierNumber;
    }

    @Override
    public void queueTask(QueuableTask t) throws TaskQueueException {
        getOrCreateBucket(t).queueTask(t);
    }

    @Override
    public Assignable<QueuableTask> nextTaskToLaunch() throws TaskQueueException {
        for (QueueBucket bucket : sortedBuckets.getSortedList()) {
            final Assignable<QueuableTask> taskOrFailure = bucket.nextTaskToLaunch();
            if (taskOrFailure != null) {
                if (taskOrFailure.hasFailure()) {
                    return taskOrFailure;
                }
                QueuableTask task = taskOrFailure.getTask();
                if (bucket.hasGuaranteedCapacityFor(task)) {
                    return taskOrFailure;
                }
                if (remainingResources == null || ResAllocsUtil.isBounded(task, remainingResources)) {
                    return taskOrFailure;
                }
                return Assignable.error(task, new AssignmentFailure(VMResource.ResAllocs, 0, 0, 0,
                        "No guaranteed capacity left for queue."
                                + "\n" + bucket.getBucketCapacityAsString()
                                + "\n" + getTierCapacityAsString()
                ));
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
            addUsage(bucket, t);
        } finally {
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
            bucket = new QueueBucket(tierNumber, bucketName, totals, allocsShareGetter);
        }
        try {
            if (bucket.launchTask(t)) {
                addUsage(bucket, t);
                return true;
            }
        } finally {
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
            for (int i = 1; i < list.size(); i++) {
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
                removeUsage(bucket, removed);
            }
        } finally {
            if (bucket.size() > 0 || (tierSla != null && tierSla.getBucketAllocs(bucket.getName()) != null))
                sortedBuckets.add(bucket);
        }
        return removed;
    }

    private void addUsage(QueueBucket bucket, QueuableTask t) {
        totals.addUsage(t);
        updateEffectiveBucketTotals(bucket);
    }

    private void removeUsage(QueueBucket bucket, QueuableTask removed) {
        totals.remUsage(removed);
        updateEffectiveBucketTotals(bucket);
    }

    private void updateEffectiveBucketTotals(QueueBucket bucket) {
        ResAllocs lastEffective = lastEffectiveUsedResources.get(bucket.getName());
        if (lastEffective != null) {
            effectiveUsedResources = ResAllocsUtil.subtract(effectiveUsedResources, lastEffective);
        }
        lastEffectiveUsedResources.put(bucket.getName(), bucket.getEffectiveUsage());
        effectiveUsedResources = ResAllocsUtil.add(effectiveUsedResources, bucket.getEffectiveUsage());

        if (tierResources == null) {
            remainingResources = null;
        } else {
            remainingResources = ResAllocsUtil.subtract(tierResources, effectiveUsedResources);
        }
    }

    @Override
    public double getDominantUsageShare() {
        return 0.0; // undefined for a tier
    }

    @Override
    public void setTaskReadyTime(String taskId, QAttributes qAttributes, long when) throws TaskQueueException {
        final QueueBucket bucket = sortedBuckets.get(qAttributes.getBucketName());
        if (bucket != null)
            bucket.setTaskReadyTime(taskId, qAttributes, when);
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
        for (QueueBucket bucket : sortedBuckets.getSortedList()) {
            bucket.reset();
        }
    }

    private String getSortedListString() {
        StringBuilder b = new StringBuilder("Tier " + tierNumber + " sortedBs: [");
        for (QueueBucket bucket : sortedBuckets.getSortedList()) {
            b.append(bucket.getName()).append(" (").append(bucket.getDominantUsageShare()).append("), ");
        }
        b.append("]");
        return b.toString();
    }

    private String getTierCapacityAsString() {
        StringBuilder sb = new StringBuilder();
        if (tierResources != null) {
            sb.append("Tier ").append(tierNumber).append(" Total Capacity: ").append(tierResources.getAsString());
        }
        if (effectiveUsedResources != null) {
            sb.append("\nTier ").append(tierNumber).append(" Used Capacity: ").append(effectiveUsedResources.getAsString());
        }
        if (remainingResources != null) {
            sb.append("\nTier ").append(tierNumber).append(" Remaining Capacity: ").append(remainingResources.getAsString());
        }
        return sb.toString();
    }

    @Override
    public void setTotalResources(Map<VMResource, Double> totalResourcesMap) {
        if (totalResMapChanged(currTotalResourcesMap, totalResourcesMap)) {
            currTotalResourcesMap.clear();
            currTotalResourcesMap.putAll(totalResourcesMap);
            for (QueueBucket b : sortedBuckets.getSortedList()) {
                b.setTotalResources(tierResources);
            }
            logger.info("Re-sorting buckets in tier " + tierNumber + " after totals changed");
            sortedBuckets.resort();
        }
    }

    private boolean totalResMapChanged(Map<VMResource, Double> currTotalResourcesMap, Map<VMResource, Double> totalResourcesMap) {
        if (currTotalResourcesMap.size() != totalResourcesMap.size())
            return true;
        Set<VMResource> curr = new HashSet<>(currTotalResourcesMap.keySet());
        for (VMResource r : totalResourcesMap.keySet()) {
            final Double c = currTotalResourcesMap.get(r);
            final Double n = totalResourcesMap.get(r);
            if ((c == null && n != null) || (c != null && n == null) || (n != null && !n.equals(c)))
                return true;
            curr.remove(r);
        }
        return !curr.isEmpty();
    }

    @Override
    public Map<TaskQueue.TaskState, Collection<QueuableTask>> getAllTasks() throws TaskQueueException {
        Map<TaskQueue.TaskState, Collection<QueuableTask>> result = new HashMap<>();
        for (QueueBucket bucket : sortedBuckets.getSortedList()) {
            final Map<TaskQueue.TaskState, Collection<QueuableTask>> allTasks = bucket.getAllTasks();
            if (!allTasks.isEmpty()) {
                for (TaskQueue.TaskState s : TaskQueue.TaskState.values()) {
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
