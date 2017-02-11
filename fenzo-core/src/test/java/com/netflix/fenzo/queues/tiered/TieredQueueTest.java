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
import com.netflix.fenzo.queues.InternalTaskQueue;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.TaskSchedulingService;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        QueuableTask t;
        while ((t = (QueuableTask)queue.next()) != null) {
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
        QueuableTask t;
        while ((t = (QueuableTask)queue.next()) != null) {
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
    @Test
    public void testTierSlas() throws Exception {
        InternalTaskQueue queue = new TieredQueue(3);
        QAttributes tier1bktA = new QAttributes.QAttributesAdaptor(0, "A");
        QAttributes tier1bktB = new QAttributes.QAttributesAdaptor(0, "B");
        QAttributes tier1bktC = new QAttributes.QAttributesAdaptor(0, "C");
        Map<Integer, Map<String, ResAllocs>> tierAllocs = getTierAllocsForThreeBuckets(tier1bktA, tier1bktB, tier1bktC);
        queue.setSla(new TieredQueueSlas(tierAllocs));
        final AtomicInteger countA = new AtomicInteger();
        final AtomicInteger countB = new AtomicInteger();
        final AtomicInteger countC = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(40);
        final TaskSchedulingService schedulingService = getSchedulingService(queue, getScheduler(),
                result -> {
                    if (!result.getResultMap().isEmpty()) {
                        for (Map.Entry<String, VMAssignmentResult> entry: result.getResultMap().entrySet()) {
                            for (TaskAssignmentResult t: entry.getValue().getTasksAssigned()) {
                                latch.countDown();
                                switch (((QueuableTask) t.getRequest()).getQAttributes().getBucketName()) {
                                    case "A":
                                        countA.incrementAndGet();
                                        break;
                                    case "B":
                                        countB.incrementAndGet();
                                        break;
                                    case "C":
                                        countC.incrementAndGet();
                                        break;
                                }
                            }
                        }
                    }
                }
        );
        // create 25 1-cpu tasks for each bucket
        for (int i=0; i<25; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        for (int i=0; i<25; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        for (int i=0; i<25; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktC, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        schedulingService.addLeases(LeaseProvider.getLeases(10, 4.0, 4000.0, 4000.0, 1, 100));
        schedulingService.start();
        Assert.assertTrue("Timeout waiting for assignments", latch.await(2, TimeUnit.SECONDS));
        Assert.assertEquals(10, countA.get());
        Assert.assertEquals(10, countC.get());
        Assert.assertEquals(20, countB.get());
    }

    // Returns a map with key=tier number (only uses tier 0) and value of a map with keys=bucket names (A, B, and C),
    // and values of ResAllocs. A has 10 cpus, B has 20 cpus, and C has 10 cpus. Memory, network and disk are 100x the
    // the number of cpus.
    private Map<Integer, Map<String, ResAllocs>> getTierAllocsForThreeBuckets(QAttributes A, QAttributes B, QAttributes C) {
        Map<Integer, Map<String, ResAllocs>> tierAllocs = new HashMap<>();
        tierAllocs.put(0, new HashMap<>());
        tierAllocs.get(0).put(A.getBucketName(),
                new ResAllocsBuilder(A.getBucketName())
                .withCores(10)
                .withMemory(1000)
                .withNetworkMbps(1000)
                .withDisk(1000)
                .build()
        );
        tierAllocs.get(0).put(B.getBucketName(),
                new ResAllocsBuilder(B.getBucketName())
                        .withCores(20)
                        .withMemory(2000)
                        .withNetworkMbps(2000)
                        .withDisk(2000)
                        .build()
        );
        tierAllocs.get(0).put(C.getBucketName(),
                new ResAllocsBuilder(C.getBucketName())
                        .withCores(10)
                        .withMemory(1000)
                        .withNetworkMbps(1000)
                        .withDisk(1000)
                        .build()
        );
        return tierAllocs;
    }

    private TaskSchedulingService getSchedulingService(TaskQueue queue, TaskScheduler scheduler,
                                                       Action1<SchedulingResult> resultCallback) {
        return new TaskSchedulingService.Builder()
                .withTaskQuue(queue)
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