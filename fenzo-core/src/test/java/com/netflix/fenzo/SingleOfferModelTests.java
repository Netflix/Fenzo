/*
 * Copyright 2015 Netflix, Inc.
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

package com.netflix.fenzo;

import com.netflix.fenzo.functions.Action1;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class SingleOfferModelTests {
    private TaskScheduler taskScheduler;
    @Before
    public void setUp() throws Exception {
        taskScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withSingleOfferPerVM(true)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                    }
                })
                .build();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSingleOfferMultipleIterations() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        taskScheduler.getTaskAssigner().call(taskRequests.get(0), resultMap.keySet().iterator().next());
        leases.clear();
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        Assert.assertEquals(3, resultMap.entrySet().iterator().next().getValue().getTasksAssigned().size());
        for(Map.Entry<String, VMAssignmentResult> entry: resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                taskScheduler.getTaskAssigner().call(r.getRequest(), entry.getKey());
            }
        }
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.size());
    }

    @Test
    public void testWithInitialJobsAlreadyOnHost() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        TaskRequest t = TaskRequestProvider.getTaskRequest(1, 10, 1);
        // indicate that the host already has one task running on it.
        taskScheduler.getTaskAssigner().call(t, leases.get(0).hostname());
        // now send the one time resource offer to scheduler
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskScheduler.scheduleOnce(taskRequests, leases);
        leases.clear();
        // now create 3 other tasks to fill rest of the host
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        Assert.assertEquals(3, resultMap.entrySet().iterator().next().getValue().getTasksAssigned().size());
        for(Map.Entry<String, VMAssignmentResult> entry: resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                taskScheduler.getTaskAssigner().call(r.getRequest(), entry.getKey());
            }
        }
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.size());
    }

    @Test
    public void testMultipleOffers() throws Exception {
        VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", 4, 4000, 1, 10);
        final TaskRequest taskRequest = TaskRequestProvider.getTaskRequest(1, 1000, 1);
        taskScheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), Collections.singletonList(host1));
        host1 = LeaseProvider.getLeaseOffer("host1", 4, 4000, 1, 10);
        SchedulingResult result = taskScheduler.scheduleOnce(Collections.singletonList(taskRequest), Collections.singletonList(host1));
        final Map<String, VMAssignmentResult> resultMap = result.getResultMap();
        for(Map.Entry<String, VMAssignmentResult> e: resultMap.entrySet()) {
            Assert.assertEquals(1, e.getValue().getLeasesUsed().size());
        }
    }
}
