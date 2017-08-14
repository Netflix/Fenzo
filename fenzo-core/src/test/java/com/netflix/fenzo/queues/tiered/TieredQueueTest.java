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

import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action0;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.queues.*;
import com.netflix.fenzo.TaskSchedulingService;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;

public class TieredQueueTest {

    @Test
    public void testAddingTasks() throws Exception {
        InternalTaskQueue queue = new TieredQueue(3);
        QAttributes tier1bktA = new QAttributes.QAttributesAdaptor(0, "A");
        QAttributes tier1bktB = new QAttributes.QAttributesAdaptor(0, "B");
        QAttributes tier2bktC = new QAttributes.QAttributesAdaptor(1, "C");
        QAttributes tier2bktD = new QAttributes.QAttributesAdaptor(1, "D");
        QAttributes tier3bktE = new QAttributes.QAttributesAdaptor(2, "E");

        int tier1=3;
        int tier2=3;
        int tier3=1;
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier2bktC, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier2bktD, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier2bktC, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier3bktE, TaskRequestProvider.getTaskRequest(1, 100, 1)));

        queue.reset();
        Assignable<QueuableTask> taskOrFailure;
        while ((taskOrFailure = (Assignable<QueuableTask>)queue.next()) != null) {
            QueuableTask t = taskOrFailure.getTask();
            switch (t.getQAttributes().getTierNumber()) {
                case 0:
                    tier1--;
                    break;
                case 1:
                    tier2--;
                    break;
                case 2:
                    tier3--;
            }
//            System.out.println("id; " + t.getId() +
//                    ", tier: " + t.getQAttributes().getTierNumber() +
//                    ", bkt: " + t.getQAttributes().getBucketName()
//            );
        }
        Assert.assertEquals(0, tier1);
        Assert.assertEquals(0, tier2);
        Assert.assertEquals(0, tier3);
    }

    @Test
    public void testAddRunningTasks() throws Exception {
        InternalTaskQueue queue = new TieredQueue(3);
        QAttributes tier1bktA = new QAttributes.QAttributesAdaptor(0, "A");
        QAttributes tier1bktB = new QAttributes.QAttributesAdaptor(0, "B");
        QAttributes tier1bktC = new QAttributes.QAttributesAdaptor(0, "C");

        final TaskScheduler scheduler = getScheduler();
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler,
                schedulingResult -> System.out.println("Got scheduling result with " +
                        schedulingResult.getResultMap().size() + " results"));

        int A1=0;
        int B1=0;
        int C1=0;
        for (int i=0; i<2; i++)
            schedulingService.initializeRunningTask(
                    QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(2, 2000, 1)),
                    "hostA"
            );
        for (int i=0; i<4; i++)
            schedulingService.initializeRunningTask(
                    QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(2, 2000, 1)),
                    "hostB"
            );
        for (int i=0; i<3; i++)
            schedulingService.initializeRunningTask(
                    QueuableTaskProvider.wrapTask(tier1bktC, TaskRequestProvider.getTaskRequest(2, 2000, 1)),
                    "hostC"
            );
        for(int i=0; i<4; i++, A1++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(2, 2000, 1)));
        for(int i=0; i<4; i++, B1++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(2, 2000, 1)));
        for(int i=0; i<4; i++, C1++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktC, TaskRequestProvider.getTaskRequest(2, 2000, 1)));

        queue.reset();
        Assignable<QueuableTask> taskOrFailure;
        while ((taskOrFailure = (Assignable<QueuableTask>)queue.next()) != null) {
            QueuableTask t = taskOrFailure.getTask();
            switch (t.getQAttributes().getBucketName()) {
                case "A":
                    A1--;
                    break;
                case "B":
                    B1--;
                    break;
                case "C":
                    C1--;
                    break;
            }
//            System.out.println("id; " + t.getId() +
//                    ", tier: " + t.getQAttributes().getTierNumber() +
//                    ", bkt: " + t.getQAttributes().getBucketName()
//            );
        }
        Assert.assertEquals(0, A1);
        Assert.assertEquals(0, B1);
        Assert.assertEquals(0, C1);
    }

    // Test weighted DRF across buckets of a tier when SLAs are given
    // Set up 3 buckets and ensure that the resources (which are less than the total requested from all 3 buckets) are
    // assigned to the buckets with the ratio equal to the ratio of the buckets' tier SLAs.
    @Test
    public void testTierSlas() throws Exception {
        TaskQueue queue = new TieredQueue(2);
        Map<String, Double> bucketWeights = new HashMap<>();
        bucketWeights.put("A", 1.0);
        bucketWeights.put("B", 2.0);
        bucketWeights.put("C", 1.0);
        queue.setSla(new TieredQueueSlas(Collections.emptyMap(), getTierAllocsForBuckets(bucketWeights)));
        final AtomicInteger countA = new AtomicInteger();
        final AtomicInteger countB = new AtomicInteger();
        final AtomicInteger countC = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(40); // 40 1-cpu slots based on adding leases below
        final TaskSchedulingService schedulingService = setupSchedSvcWithAssignmentCounters(queue, new AtomicReference<>(latch), countA, countB, countC);
        // create 25 1-cpu tasks for each bucket
        createTasksForBuckets(queue, 25, "A", "B", "C");
        schedulingService.addLeases(LeaseProvider.getLeases(10, 4.0, 4000.0, 4000.0, 1, 100));
        schedulingService.start();
        Assert.assertTrue("Timeout waiting for assignments", latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(500); // Additional way due to race conditions

        Assert.assertEquals(10, countA.get());
        Assert.assertEquals(20, countB.get());
        Assert.assertEquals(10, countC.get());
    }

    // Test that weighted DRF is maintained beyond initial assignments when tiered queue sla is modified.
    // Set up 3 buckets, ensure that resources are assigned based on the tiered slas, similar to how it is done in
    // another test, testTierSlas(). Then, change the queue slas to alter weights from A=1,B=2,C=1 to A=1,B=1,C=2.
    // Add more tasks and agents such that there are more tasks from each bucket than can be assigned. Ensure weighted
    // DRF now reflects the new weights.
    @Test
    public void testTierSlas2() throws Exception {
        TaskQueue queue = new TieredQueue(2);
        Map<String, Double> bucketWeights = new HashMap<>();
        bucketWeights.put("A", 1.0);
        bucketWeights.put("B", 2.0);
        bucketWeights.put("C", 1.0);

        ResAllocs tier1Capacity = new ResAllocsBuilder("tier#0")
                .withCores(10 * 4)
                .withMemory(10 * 4000)
                .withNetworkMbps(10 * 4000)
                .build();
        queue.setSla(new TieredQueueSlas(singletonMap(1, tier1Capacity), getTierAllocsForBuckets(bucketWeights)));

        final AtomicInteger countA = new AtomicInteger();
        final AtomicInteger countB = new AtomicInteger();
        final AtomicInteger countC = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(40);
        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(latch);
        final TaskSchedulingService schedulingService = setupSchedSvcWithAssignmentCounters(queue, latchRef, countA, countB, countC);
        // create 25 1-cpu tasks for each bucket
        createTasksForBuckets(queue, 25, "A", "B", "C");
        schedulingService.addLeases(LeaseProvider.getLeases(10, 4.0, 4000.0, 4000.0, 1, 100));
        schedulingService.start();
        Assert.assertTrue("Timeout waiting for assignments", latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(500); // Additional way due to race conditions

        Assert.assertEquals(10, countA.get());
        Assert.assertEquals(20, countB.get());
        Assert.assertEquals(10, countC.get());
        // change the weights
        bucketWeights.clear();
        bucketWeights.put("A", 1.0);
        bucketWeights.put("B", 1.0);
        bucketWeights.put("C", 2.0);

        // Double the tier capacity
        ResAllocs doubledTier1Capacity = new ResAllocsBuilder("tier#0")
                .withCores(20 * 4)
                .withMemory(20 * 4000)
                .withNetworkMbps(20 * 4000)
                .build();
        queue.setSla(new TieredQueueSlas(singletonMap(0, doubledTier1Capacity), getTierAllocsForBuckets(bucketWeights)));

        // reset latch to new one
        latchRef.set(new CountDownLatch(40));
        // reset counters
        countA.set(0); countB.set(0); countC.set(0);
        // create additional 25 1-cpu tasks each
        createTasksForBuckets(queue, 25, "A", "B", "C");
        // add new leases
        schedulingService.addLeases(LeaseProvider.getLeases(10, 4.0, 4000.0, 4000.0, 1, 100));
        Assert.assertTrue("Timeout waiting for assignments", latchRef.get().await(2, TimeUnit.SECONDS));
        Thread.sleep(500); // Additional way due to race conditions

        Assert.assertEquals(10, countA.get()); // total 20
        Assert.assertEquals(0, countB.get());  // total 20
        Assert.assertEquals(30, countC.get()); // total 40
    }

    // Test that weighted DRF is maintained correctly even when starting with a scheduler initialized with previously
    // running tasks
    @Test
    public void testTierSlasWithPreviouslyRunningTasks() throws Exception {
        TaskQueue queue = new TieredQueue(2);
        Map<String, Double> bucketWeights = new HashMap<>();
        bucketWeights.put("A", 1.0);
        bucketWeights.put("B", 2.0);
        bucketWeights.put("C", 1.0);
        queue.setSla(new TieredQueueSlas(Collections.emptyMap(), getTierAllocsForBuckets(bucketWeights)));
        final AtomicInteger countA = new AtomicInteger();
        final AtomicInteger countB = new AtomicInteger();
        final AtomicInteger countC = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(32);
        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(latch);
        final TaskSchedulingService schedulingService = setupSchedSvcWithAssignmentCounters(queue, latchRef, countA, countB, countC);
        initializeRunningTasksForBuckets(schedulingService, 5, "A");
        initializeRunningTasksForBuckets(schedulingService, 3, "C");
        createTasksForBuckets(queue, 25, "A", "B", "C");
        schedulingService.addLeases(LeaseProvider.getLeases(8, 4.0, 4000.0, 4000.0, 1, 100));
        schedulingService.start();
        Assert.assertTrue("Timeout waiting for assignments", latchRef.get().await(2, TimeUnit.SECONDS));
        Thread.sleep(500); // Additional way due to race conditions

        Assert.assertEquals(10-5, countA.get());
        Assert.assertEquals(20, countB.get());
        Assert.assertEquals(10-3, countC.get());
    }

    @Test
    public void testTierSlasWithOneBucketWithNoTasks() throws Exception {
        TaskQueue queue = new TieredQueue(2);
        Map<String, Double> bucketWeights = new HashMap<>();
        bucketWeights.put("A", 1.0);
        bucketWeights.put("B", 2.0);
        bucketWeights.put("C", 1.0);

        ResAllocs tier1Capacity = new ResAllocsBuilder("tier#0")
                .withCores(70 * 4)
                .withMemory(70 * 4000)
                .withNetworkMbps(70 * 4000)
                .build();
        queue.setSla(new TieredQueueSlas(singletonMap(0, tier1Capacity), getTierAllocsForBuckets(bucketWeights)));

        final AtomicInteger countA = new AtomicInteger();
        final AtomicInteger countB = new AtomicInteger();
        final AtomicInteger countC = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(40);
        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(latch);
        final TaskSchedulingService schedulingService = setupSchedSvcWithAssignmentCounters(queue, latchRef, countA, countB, countC);
        // create 25 1-cpu tasks for each bucket
        createTasksForBuckets(queue, 25, "A", "C");
        schedulingService.addLeases(LeaseProvider.getLeases(10, 4.0, 4000.0, 4000.0, 1, 100));
        schedulingService.start();
        Assert.assertTrue("Timeout waiting for assignments", latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(500); // Additional way due to race conditions

        Assert.assertEquals(20, countA.get());
        Assert.assertEquals(0, countB.get());
        Assert.assertEquals(20, countC.get());
    }

    private void createTasksForBuckets(TaskQueue queue, int numTasks, String... bucketNames) {
        for (String b: bucketNames) {
            QAttributes tier1bkt = new QAttributes.QAttributesAdaptor(0, b);
            for (int i = 0; i < numTasks; i++)
                queue.queueTask(QueuableTaskProvider.wrapTask(tier1bkt, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        }
    }

    // initializes scheduling service with given number of tasks for each bucket name. Sets the host name for each running
    // task to a random hostname.
    private void initializeRunningTasksForBuckets(TaskSchedulingService schedulingService, int numTasks, String... bucketNames) {
        Random r = new Random(System.currentTimeMillis());
        for (String b: bucketNames) {
            QAttributes tier1bkt = new QAttributes.QAttributesAdaptor(0, b);
            for (int i = 0; i < numTasks; i++)
                schedulingService.initializeRunningTask(
                        QueuableTaskProvider.wrapTask(tier1bkt, TaskRequestProvider.getTaskRequest(1, 100, 1)), "host" + r.nextInt());
        }
    }

    // sets up scheduling service and increments counters for each bucket name, where names are assumed to
    // upper case letters starting from A. That is, the first counter is incremented for A, 2nd for B, etc. The latch
    // is counted down for each task assignment.
    private TaskSchedulingService setupSchedSvcWithAssignmentCounters(TaskQueue queue, AtomicReference<CountDownLatch> latch, AtomicInteger... counters) {
        return getSchedulingService(queue, getScheduler(),
                result -> {
                    if (!result.getResultMap().isEmpty()) {
                        for (Map.Entry<String, VMAssignmentResult> entry: result.getResultMap().entrySet()) {
                            for (TaskAssignmentResult t: entry.getValue().getTasksAssigned()) {
                                latch.get().countDown();
                                int idx = ((QueuableTask) t.getRequest()).getQAttributes().getBucketName().charAt(0) - 'A';
                                counters[idx].incrementAndGet();
                            }
                        }
                    }
                }
        );
    }

    // Returns a map with key=tier number (only uses tier 0) and value of a map with keys=bucket names (A, B, and C),
    // and values of ResAllocs. A has 10 cpus, B has 20 cpus, and C has 10 cpus. Memory, network and disk are 100x the
    // the number of cpus.
    private Map<Integer, Map<String, ResAllocs>> getTierAllocsForBuckets(Map<String, Double> bucketWeights) {
        Map<Integer, Map<String, ResAllocs>> tierAllocs = new HashMap<>();
        tierAllocs.put(0, new HashMap<>());
        for (Map.Entry<String, Double> entry: bucketWeights.entrySet()) {
            tierAllocs.get(0).put(entry.getKey(),
                    new ResAllocsBuilder(entry.getKey())
                            .withCores(10 * entry.getValue())
                            .withMemory(1000 * entry.getValue())
                            .withNetworkMbps(1000 * entry.getValue())
                            .withDisk(1000 * entry.getValue())
                            .build()
            );
        }
        return tierAllocs;
    }

    private TaskSchedulingService getSchedulingService(TaskQueue queue, TaskScheduler scheduler,
                                                       Action1<SchedulingResult> resultCallback) {
        return new TaskSchedulingService.Builder()
                .withTaskQueue(queue)
                .withLoopIntervalMillis(100L)
                .withPreSchedulingLoopHook(new Action0() {
                    @Override
                    public void call() {
                        System.out.println("Pre-scheduling hook");
                    }
                })
                .withSchedulingResultCallback(resultCallback)
                .withTaskScheduler(scheduler)
                .build();
    }

    private TaskScheduler getScheduler() {
        return new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                    }
                })
                .build();
    }
}