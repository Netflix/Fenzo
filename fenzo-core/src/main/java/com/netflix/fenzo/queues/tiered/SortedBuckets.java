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

import com.netflix.fenzo.queues.UsageTrackedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The buckets are sorted using a comparison method that makes it inconsistent with equals. This class maintains
 * the ordering while being able to also perform methods such as contains, add, and remove without depending on
 * comparisons being strictly consistent with equals. Comparisons use the bucket's resource usage values, where as
 * the equals is defined by the bucket name being equal. Duplicate entries with the same bucket name are not allowed.
 * There may be multiple buckets with the same resource usage values, and therefore, comparator returns 0 while the
 * equals check returns {@code true} only when the bucket name matches. The Collections classes such as SortedMap or
 * SortedSet cannot be used due to this inconsistency.
 * <P>
 * This implementation provides {@code O(logN)} performance for adding an item, constant-time performance for get,
 * and {@code O(logN+M)} performance for remove, where, {@code N} is the number of items in the collection and
 * {@code M} is the average number of items with same resource usage values.
 * <P>
 * This implementation is not synchronized. Invocations of methods of this class must be synchronized externally if
 * there is a chance of calling them concurrently.
 */
class SortedBuckets {
    // TODO performance can be improved by changing List<> here to a two level map - outer map will have keys
    // of bucket's resource usage and values will be a Map<String, QueueBucket>.
    private static final Logger logger = LoggerFactory.getLogger(SortedBuckets.class);
    private final List<QueueBucket> buckets;
    private final Map<String, QueueBucket> bucketMap;
    private final Comparator<QueueBucket> comparator;
    private final UsageTrackedQueue.ResUsage parentUsage;

    SortedBuckets(final UsageTrackedQueue.ResUsage parentUsage) {
        buckets = new ArrayList<>();
        bucketMap = new HashMap<>();
        comparator = new Comparator<QueueBucket>() {
            @Override
            public int compare(QueueBucket o1, QueueBucket o2) {
                return Double.compare(o1.getDominantUsageShare(), o2.getDominantUsageShare());
            }
        };
        this.parentUsage = parentUsage;
    }

    boolean add(QueueBucket bucket) {
        if (bucketMap.containsKey(bucket.getName()))
            return false;
        if (buckets.isEmpty())
            buckets.add(bucket);
        else
            buckets.add(findInsertionPoint(bucket, buckets), bucket);
        bucketMap.put(bucket.getName(), bucket);
        return true;
    }

    QueueBucket remove(String bucketName) {
        final QueueBucket bucket = bucketMap.get(bucketName);
        if (bucket == null)
            return null;
        final int index = findInsertionPoint(bucket, buckets);
        if (index < 0)
            throw new IllegalStateException("Unexpected: bucket with name=" + bucketName + " does not exist");
        // we have now found a bucket that has the same position due to its usage value. The actual bucket we are
        // interested in (with the same name) may be the same one or it may be to the left or right a few positions.
        int remPos = buckets.get(index).getName().equals(bucketName)? index : -1;
        if (remPos < 0)
            remPos = findWalkingLeft(buckets, index, bucketName, bucket.getDominantUsageShare());
        if (remPos < 0)
            remPos = findWalkingRight(buckets, index, bucketName, bucket.getDominantUsageShare());
        if (remPos < 0) {
            logger.error("Unexpected: bucket with name=" + bucketName + " not found to remove, traversing " +
                    buckets.size() + " buckets to remove it");
            logger.warn("Invalid sorted buckets list: " + getBucketsListString());
            removeBucketAndResort(bucketName);
        }
        else
            buckets.remove(remPos);
        bucketMap.remove(bucketName);
        return bucket;
    }

    private void removeBucketAndResort(String bucketName) {
        // workaround the problem: linear traversal of the list to remove the bucket and re-sort if needed
        // also check on uniqueness of bucket names in the list
        final HashSet<String> names = new HashSet<>();
        if (!buckets.isEmpty()) {
            final Iterator<QueueBucket> iterator = buckets.iterator();
            QueueBucket prev = null;
            boolean isSorted = true;
            while (iterator.hasNext()) {
                QueueBucket b = iterator.next();
                if (!names.add(b.getName())) {
                    logger.error("Bucket " + b.getName() + " already existed in the list, removing");
                    isSorted = false;
                    iterator.remove();
                }
                else if (b.getName().equals(bucketName)) {
                    iterator.remove();
                } else {
                    if (prev != null) {
                        final int compare = comparator.compare(prev, b);
                        isSorted = isSorted && compare <= 0;
                    }
                    prev = b;
                }
            }
            logger.warn("Re-sorting buckets list");
            resort();
        }
    }

    private String getBucketsListString() {
        StringBuilder builder = new StringBuilder("[");
        for (QueueBucket b: buckets) {
            builder.append(b.getName()).append(":").append(b.getDominantUsageShare()).append(", ");
        }
        builder.append("]");
        return builder.toString();
    }

    QueueBucket get(String bucketName) {
        return bucketMap.get(bucketName);
    }

    private int findWalkingRight(List<QueueBucket> buckets, int index, String bucketName, double dominantUsageShare) {
        int pos = index;
        while (++pos < buckets.size() && buckets.get(pos).getDominantUsageShare() == dominantUsageShare) {
            if (buckets.get(pos).getName().equals(bucketName))
                return pos;
        }
        return -1;
    }

    private int findWalkingLeft(List<QueueBucket> buckets, int index, String bucketName, double dominantUsageShare) {
        int pos = index;
        while (--pos >= 0 && buckets.get(pos).getDominantUsageShare() == dominantUsageShare) {
            if (buckets.get(pos).getName().equals(bucketName))
                return pos;
        }
        return -1;
    }

    private int findInsertionPoint(QueueBucket bucket, List<QueueBucket> buckets) {
        final int i = Collections.binarySearch(buckets, bucket, comparator);
        if (i >= 0)
            return i;
        return -i - 1;
    }

    List<QueueBucket> getSortedList() {
        return Collections.unmodifiableList(buckets);
    }

    void resort() {
        List<QueueBucket> old = new ArrayList<>(buckets);
        bucketMap.clear();
        buckets.clear();
        for(QueueBucket b: old)
            add(b);
    }
}
