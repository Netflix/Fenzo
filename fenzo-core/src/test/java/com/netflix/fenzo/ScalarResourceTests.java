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

package com.netflix.fenzo;

import com.netflix.fenzo.functions.Action1;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ScalarResourceTests {

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

    // Test that asking for a scalar resource gets the task assigned
    @Test
    public void testScalar1() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final Map<String, Double> scalars = new HashMap<>();
        scalars.put("gpu", 1.0);
        final TaskRequest task = TaskRequestProvider.getTaskRequest(null, 1, 100, 1, 1, 1, null, null, null, scalars);
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", 4.0, 4000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, scalars);
        final SchedulingResult result = scheduler.scheduleOnce(Collections.singletonList(task), Collections.singletonList(host1));
        Assert.assertEquals(0, result.getFailures().size());
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(task.getId(), result.getResultMap().values().iterator().next().getTasksAssigned().iterator().next().getRequest().getId());
    }

    // Test that asking for more than available scalar resources does not get the task assigned
    @Test
    public void testInsufficientScalarResources() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final double scalarsOnHost=4.0;
        final TaskRequest task = TaskRequestProvider.getTaskRequest(null, 1, 100, 1, 1, 1, null, null, null, Collections.singletonMap("gpu", scalarsOnHost+1.0));
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", 4.0, 4000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, Collections.singletonMap("gpu", scalarsOnHost));
        final SchedulingResult result = scheduler.scheduleOnce(Collections.singletonList(task), Collections.singletonList(host1));
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(0, result.getResultMap().size());
        Assert.assertEquals(VMResource.Other, result.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource());
        System.out.println(result.getFailures().values().iterator().next().get(0).getFailures().get(0));
    }

    @Test
    public void testMultipleTasksScalarRequests() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final double scalarsOnHost = 4.0;
        final List<TaskRequest> tasks = new ArrayList<>();
        for (int i = 0; i < scalarsOnHost + 1; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(null, 1, 100, 1, 1, 1, null, null, null, Collections.singletonMap("gpu", 1.0)));
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", scalarsOnHost*2.0, scalarsOnHost*2000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, Collections.singletonMap("gpu", scalarsOnHost));
        final SchedulingResult result = scheduler.scheduleOnce(tasks, Collections.singletonList(host1));
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals((int)scalarsOnHost, result.getResultMap().values().iterator().next().getTasksAssigned().size());
        Assert.assertEquals(VMResource.Other, result.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource());
    }

    @Test
    public void testScalarRequestsAcrossHosts() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final double scalarsOnHost = 4.0;
        final List<TaskRequest> tasks = new ArrayList<>();
        for (int i = 0; i < scalarsOnHost * 2; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(null, 1, 100, 1, 1, 1, null, null, null, Collections.singletonMap("gpu", 1.0)));
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", scalarsOnHost, scalarsOnHost*1000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, Collections.singletonMap("gpu", scalarsOnHost));
        final VirtualMachineLease host2 = LeaseProvider.getLeaseOffer("host2", scalarsOnHost, scalarsOnHost*1000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, Collections.singletonMap("gpu", scalarsOnHost));
        final SchedulingResult result = scheduler.scheduleOnce(tasks, Arrays.asList(host1, host2));
        Assert.assertEquals(0, result.getFailures().size());
        Assert.assertEquals(2, result.getResultMap().size());
        int tasksAssigned=0;
        for(VMAssignmentResult r: result.getResultMap().values()) {
            tasksAssigned += r.getTasksAssigned().size();
        }
        Assert.assertEquals(tasks.size(), tasksAssigned);
    }

    // test allocation of multiple scalar resources
    @Test
    public void testMultipleScalarResources() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final double scalars1OnHost = 4.0;
        final double scalars2OnHost = 2.0;
        final Map<String, Double> scalarReqs = new HashMap<>();
        scalarReqs.put("gpu", 1.0);
        scalarReqs.put("foo", 1.0);
        final List<TaskRequest> tasks = new ArrayList<>();
        for(int i=0; i<scalars1OnHost*2; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(null, 1, 100, 1, 1, 1, null, null, null, scalarReqs));
        final Map<String, Double> scalarResources = new HashMap<>();
        scalarResources.put("gpu", scalars1OnHost);
        scalarResources.put("foo", scalars2OnHost);
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", scalars1OnHost*2.0, scalars1OnHost*2000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, scalarResources);
        final VirtualMachineLease host2 = LeaseProvider.getLeaseOffer("host2", scalars1OnHost*2.0, scalars1OnHost*2000.0, 100, 1024,
                Collections.singletonList(new VirtualMachineLease.Range(1, 10)), null, scalarResources);
        final SchedulingResult result = scheduler.scheduleOnce(tasks, Arrays.asList(host1, host2));
        Assert.assertEquals((int)(scalars1OnHost/scalars2OnHost)*2, result.getFailures().size());
        Assert.assertEquals(2, result.getResultMap().size());
        int tasksAssigned=0;
        for(VMAssignmentResult r: result.getResultMap().values()) {
            tasksAssigned += r.getTasksAssigned().size();
        }
        Assert.assertEquals((int)(scalars2OnHost*2.0), tasksAssigned);
    }
}
