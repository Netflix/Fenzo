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
import junit.framework.Assert;
import org.junit.Test;

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
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler);

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

    private TaskSchedulingService getSchedulingService(TaskQueue queue, TaskScheduler scheduler) {
        return new TaskSchedulingService.Builder()
                .withTaskQuue(queue)
                .withLoopIntervalMillis(1000L)
                .withPreSchedulingLoopHook(new Action0() {
                    @Override
                    public void call() {
                        System.out.println("Pre-scheduling hook");
                    }
                })
                .withSchedulingResultCallback(new Action1<SchedulingResult>() {
                    @Override
                    public void call(SchedulingResult schedulingResult) {
                        System.out.println("Got scheduling result with " + schedulingResult.getResultMap().size() + " results");
                    }
                })
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