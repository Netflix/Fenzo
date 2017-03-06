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

import com.netflix.fenzo.TaskRequestProvider;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.UsageTrackedQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortedBucketsTest {

    @Test
    public void testSorting() throws Exception {
        UsageTrackedQueue.ResUsage parentUsage = new UsageTrackedQueue.ResUsage();
        QAttributes tier1bktA = new QAttributes.QAttributesAdaptor(1, "Parent");
        parentUsage.addUsage(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(100, 1000, 100)));
        SortedBuckets sortedBuckets = new SortedBuckets(parentUsage);

        final QueueBucket a = new QueueBucket(1, "A", parentUsage, null);
        final QAttributes attrA = new QAttributes.QAttributesAdaptor(1, "A");
        a.launchTask(QueuableTaskProvider.wrapTask(attrA, TaskRequestProvider.getTaskRequest(10, 100, 10)));
        sortedBuckets.add(a);

        final QueueBucket b = new QueueBucket(1, "B", parentUsage, null);
        final QAttributes attrB = new QAttributes.QAttributesAdaptor(1, "B");
        b.launchTask(QueuableTaskProvider.wrapTask(attrB, TaskRequestProvider.getTaskRequest(20, 200, 20)));
        sortedBuckets.add(b);

        final QueueBucket c = new QueueBucket(1, "C", parentUsage, null);
        final QAttributes attrC = new QAttributes.QAttributesAdaptor(1, "C");
        c.launchTask(QueuableTaskProvider.wrapTask(attrC, TaskRequestProvider.getTaskRequest(15, 150, 15)));
        sortedBuckets.add(c);

        final QueueBucket d = new QueueBucket(1, "D", parentUsage, null);
        final QAttributes attrD = new QAttributes.QAttributesAdaptor(1, "D");
        d.launchTask(QueuableTaskProvider.wrapTask(attrD, TaskRequestProvider.getTaskRequest(10, 100, 10)));
        sortedBuckets.add(d);

        final List<QueueBucket> sortedList = sortedBuckets.getSortedList();
        QueueBucket prev = sortedList.get(0);
        for (int i = 1; i < sortedList.size(); i++) {
            Assert.assertTrue(prev.getDominantUsageShare() <= sortedList.get(i).getDominantUsageShare());
            prev = sortedList.get(i);
        }
    }

    @Test
    public void testDominantResourceUsageRebalancing() throws Exception {
        TieredQueue queue = new TieredQueue(3);
        QAttributes bucketA = new QAttributes.QAttributesAdaptor(0, "A");
        QAttributes bucketB = new QAttributes.QAttributesAdaptor(0, "B");
        QAttributes bucketC = new QAttributes.QAttributesAdaptor(0, "C");

        QueuableTask taskA = QueuableTaskProvider.wrapTask(bucketA, TaskRequestProvider.getTaskRequest(2, 1, 0));
        QueuableTask taskB = QueuableTaskProvider.wrapTask(bucketB, TaskRequestProvider.getTaskRequest(1, 10, 0));
        QueuableTask taskC = QueuableTaskProvider.wrapTask(bucketC, TaskRequestProvider.getTaskRequest(1, 20, 0));

        queue.queueTask(taskA);
        queue.queueTask(taskB);

        queue.getUsageTracker().launchTask(taskA);
        queue.getUsageTracker().launchTask(taskB);

        // Adding this task breaks buckets queue ordering
        queue.queueTask(taskC);
        queue.getUsageTracker().launchTask(taskC);

        // Now add another task to bucket A - this would trigger an exception if sorting order is incorrect,
        // as was observed due to a previous bug
        QueuableTask taskA2 = QueuableTaskProvider.wrapTask(bucketA, TaskRequestProvider.getTaskRequest(2, 1, 0));
        queue.queueTask(taskA2);
        queue.getUsageTracker().launchTask(taskA2);
    }

    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(10, 20, 30, 40, 50, 60);
        Comparator<Integer> comparator = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };
        System.out.println("Inserting 15 at " + Collections.binarySearch(list, 15, comparator));
        System.out.println("Inserting 25 at " + Collections.binarySearch(list, 25, comparator));
        System.out.println("Inserting 65 at " + Collections.binarySearch(list, 65, comparator));
        System.out.println("Inserting 5 at " + Collections.binarySearch(list, 5, comparator));
        System.out.println("Inserting 20 at " + Collections.binarySearch(list, 20, comparator));
    }
}