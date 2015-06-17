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

import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.sla.ResAllocs;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Test;
import com.netflix.fenzo.functions.Action1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResAllocsTests {

    private static final String grp1="App1";
    private static final String grp2="App2";
    static String hostAttrName = "MachineType";
    final int minIdle=5;
    final int maxIdle=10;
    final long coolDownSecs=2;
    String hostAttrVal1="4coreServers";
    int cpus1=4;
    int memory1=40000;
    final AutoScaleRule rule1 = AutoScaleRuleProvider.createRule(hostAttrVal1, minIdle, maxIdle, coolDownSecs, cpus1/2, memory1/2);

    private TaskScheduler getSchedulerNoResAllocs() {
        return new TaskScheduler.Builder()
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuBinPacker)
                .withLeaseOfferExpirySecs(3600)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        Assert.fail("Unexpected to reject lease " + lease.hostname());
                    }
                })
                .build();
    }
    private TaskScheduler getScheduler() {
        Map<String, ResAllocs> resAllocs = new HashMap<>();
        resAllocs.put(grp1, ResAllocsProvider.create(grp1, 4, 4000, Double.MAX_VALUE, Double.MAX_VALUE));
        resAllocs.put(grp2, ResAllocsProvider.create(grp2, 8, 8000, Double.MAX_VALUE, Double.MAX_VALUE));
        return new TaskScheduler.Builder()
                .withInitialResAllocs(resAllocs)
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuBinPacker)
                .withLeaseOfferExpirySecs(3600)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        Assert.fail("Unexpected to reject lease " + lease.hostname());
                    }
                })
                .build();
    }
    private TaskScheduler getAutoscalingScheduler(AutoScaleRule... rules) {
        TaskScheduler.Builder builder = new TaskScheduler.Builder()
                .withAutoScaleByAttributeName(hostAttrName);
        Map<String, ResAllocs> resAllocs = new HashMap<>();
        resAllocs.put(grp1, ResAllocsProvider.create(grp1, cpus1, memory1, Double.MAX_VALUE, Double.MAX_VALUE));
        resAllocs.put(grp2, ResAllocsProvider.create(grp2, cpus1 * 2, memory1 * 2, Double.MAX_VALUE, Double.MAX_VALUE));
        for(AutoScaleRule rule: rules)
            builder.withAutoScaleRule(rule);
        return builder
                .withInitialResAllocs(resAllocs)
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuBinPacker)
                .withLeaseOfferExpirySecs(3600)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        Assert.fail("Unexpected to reject lease " + lease.hostname());
                    }
                })
                .build();
    }

    @Test
    public void testNoResAllocsMeansUnlimited() {
        final TaskScheduler scheduler = getSchedulerNoResAllocs();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(10, 4.0, 4000.0, 1024.0, 1, 100);
        final List<TaskRequest> tasks = new ArrayList<>();
        for(int i=0; i<leases.size()*4; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(1.0, 10.0, 1));
        final SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
        final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
        int successes=0;
        for(Map.Entry<String, VMAssignmentResult> entry : resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                if(r.isSuccessful())
                    successes++;
            }
        }
        Assert.assertEquals("#success assignments: ", tasks.size(), successes);
        Assert.assertEquals("#failures: ", 0, schedulingResult.getFailures().size());
    }

    @Test
    public void testSingleAppRsv() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, 4.0, 4000.0, 1024.0, 1, 100);
        final List<TaskRequest> tasks = new ArrayList<>();
        final int numCores = (int)scheduler.getResAllocs().get(grp1).getCores();
        for(int i=0; i<numCores+1; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(grp1, 1.0, 1000.0, 100.0, 1));
        final SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
        final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
        int successes=0;
        for(Map.Entry<String, VMAssignmentResult> entry : resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                if(r.isSuccessful())
                    successes++;
            }
        }
        Assert.assertEquals("#success assignments: ", numCores, successes);
        Assert.assertEquals("#failures: ", 1, schedulingResult.getFailures().size());
        final VMResource resource = schedulingResult.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource();
        Assert.assertTrue("Unexpected failure type: " + resource, resource == VMResource.ResAllocs);
    }

    @Test
    public void testTwoAppRsv() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(4, 4.0, 4000.0, 1024.0, 1, 100);
        final List<TaskRequest> tasks = new ArrayList<>();
        final int numCores = (int)scheduler.getResAllocs().get(grp1).getCores();
        for(int i=0; i<numCores+1; i++) {
            tasks.add(TaskRequestProvider.getTaskRequest(grp1, 1.0, 1000.0, 100.0, 1));
            tasks.add(TaskRequestProvider.getTaskRequest(grp2, 1.0, 1000.0, 100.0, 1));
        }
        final SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
        final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
        int grp1Success=0;
        int grp2Success=0;
        for(Map.Entry<String, VMAssignmentResult> entry : resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                if(r.isSuccessful())
                    switch (r.getRequest().taskGroupName()) {
                        case grp1:
                            grp1Success++;
                            break;
                        case grp2:
                            grp2Success++;
                            break;
                    }
            }
        }
        Assert.assertEquals(grp1Success, numCores);
        Assert.assertEquals(grp2Success, numCores+1);
        Assert.assertEquals("Incorrect #failures: ", 1, schedulingResult.getFailures().size());
        final VMResource resource = schedulingResult.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource();
        Assert.assertTrue("Unexpected failure type: " + resource, resource == VMResource.ResAllocs);
    }

    // Test that scale up isn't called when tasks from a group, grp1, are limited by its resAllocs. Then, ensure that
    // tasks of a different group, grp2, do actually invoke scale up while grp1 tasks don't.
    @Test
    public void testScaleUpForRsv() throws Exception {
        TaskScheduler scheduler = getAutoscalingScheduler(rule1);
        final List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1)).build();
        attributes.put(hostAttrName, attribute);
        for(int l=0; l<minIdle+1; l++)
            leases.add(LeaseProvider.getLeaseOffer("host"+l, cpus1, memory1, 1024.0, ports, attributes));
        final List<TaskRequest> tasks = new ArrayList<>();
        for(int t=0; t<cpus1*2; t++)
            tasks.add(TaskRequestProvider.getTaskRequest(grp1, 1, 100, 10, 1));
        final AtomicBoolean gotScaleUpRequest = new AtomicBoolean();
        scheduler.setAutoscalerCallback(new Action1<AutoScaleAction>() {
            @Override
            public void call(AutoScaleAction action) {
                if(action instanceof ScaleUpAction) {
                    int needed = ((ScaleUpAction)action).getScaleUpCount();
                    gotScaleUpRequest.set(true);
                }
            }
        });
        Set<String> assignedHosts = new HashSet<>();
        long now = System.currentTimeMillis();
        for(int i=0; i<coolDownSecs+2; i++) {
            if(i>0) {
                tasks.clear();
                leases.clear();
            }
            final SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
            if(i==0) {
                Assert.assertEquals(1, schedulingResult.getResultMap().size());
                int successes=0;
                for(Map.Entry<String, VMAssignmentResult> entry: schedulingResult.getResultMap().entrySet()) {
                    for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                        if(r.isSuccessful()) {
                            assignedHosts.add(entry.getKey());
                            successes++;
                            scheduler.getTaskAssigner().call(r.getRequest(), entry.getKey());
                        }
                    }
                }
            }
            Thread.sleep(1000);
        }
        System.out.println("mSecs taken: " + (System.currentTimeMillis()-now));
        Assert.assertEquals("Assigned hosts should have been 1, it is: " + assignedHosts, 1, assignedHosts.size());
        Assert.assertFalse("Unexpected to get scale up request", gotScaleUpRequest.get());
        for(int t=0; t<cpus1*2; t++)
            tasks.add(TaskRequestProvider.getTaskRequest(grp2, 1, 100, 10, 1));
        tasks.add(TaskRequestProvider.getTaskRequest(grp1, 1, 100, 10, 1));
        for(int i=0; i<coolDownSecs+2 && !gotScaleUpRequest.get(); i++) {
            final Map<String, VMAssignmentResult> resultMap = scheduler.scheduleOnce(tasks, leases).getResultMap();
            if(i==0) {
                int successes1=0;
                int successes2=0;
                assignedHosts.clear();
                for(Map.Entry<String, VMAssignmentResult> entry: resultMap.entrySet()) {
                    for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                        if(r.isSuccessful()) {
                            if(r.getRequest().taskGroupName().equals(grp2)) {
                                successes2++;
                                assignedHosts.add(entry.getKey());
                            }
                            else
                                successes1++;
                        }
                    }
                }
                Assert.assertEquals("Didn't expect grp1 task to be assigned", successes1, 0);
                Assert.assertEquals(successes2, tasks.size()-1);
                Assert.assertEquals(2, assignedHosts.size());
                tasks.clear();
                leases.clear();
            }
            Thread.sleep(1000);
        }
        Thread.sleep(coolDownSecs+1); // wait for scale up request to happen
        Assert.assertTrue("Didn't get scale up request", gotScaleUpRequest.get());
    }

    @Test
    public void testResAllocsModification() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final int numCores = (int)scheduler.getResAllocs().get(grp1).getCores();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, (double)numCores, numCores*1000.0, 1024.0, 1, 100);
        final List<TaskRequest> tasks = new ArrayList<>();
        for(int i=0; i<numCores; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(grp1, 1.0, 1000.0, 100.0, 1));
        SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
        Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
        int successes=0;
        for(Map.Entry<String, VMAssignmentResult> entry : resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                if(r.isSuccessful()) {
                    successes++;
                    scheduler.getTaskAssigner().call(r.getRequest(), entry.getKey());
                }
            }
        }
        Assert.assertEquals("#success assignments: ", numCores, successes);
        Assert.assertEquals("Not expecting assignment failures: ", 0, schedulingResult.getFailures().size());
        // now confirm that next task fails with resAllocs type failure
        tasks.clear();
        tasks.add(TaskRequestProvider.getTaskRequest(grp1, 1.0, 1000.0, 100.0, 1));
        leases.clear();
        schedulingResult = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals("#failures", 1, schedulingResult.getFailures().size());
        final VMResource resource = schedulingResult.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource();
        Assert.assertTrue("Unexpected failure type: " + resource, resource == VMResource.ResAllocs);
        // now increase resAllocs and confirm that the new task gets assigned
        scheduler.addOrReplaceResAllocs(ResAllocsProvider.create(grp1, numCores + 1, (numCores + 1) * 1000.0, Double.MAX_VALUE, Double.MAX_VALUE));
        schedulingResult = scheduler.scheduleOnce(tasks, leases);
        resultMap = schedulingResult.getResultMap();
        successes=0;
        for(Map.Entry<String, VMAssignmentResult> entry : resultMap.entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                if(r.isSuccessful())
                    successes++;
            }
        }
        Assert.assertEquals("Incorrect #success assignments: ", 1, successes);
        Assert.assertEquals("Not expecting assignment failures: ", 0, schedulingResult.getFailures().size());
    }

    @Test
    public void testAbsentResAllocs() throws Exception {
        final TaskScheduler scheduler = getScheduler();
        final int numCores = (int)scheduler.getResAllocs().get(grp1).getCores();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, (double)numCores, numCores*1000.0, 1024.0, 1, 100);
        final List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest("AbsentGrp", 1.0, 1000.0, 100.0, 1));
        SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals("#failures: ", 1, schedulingResult.getFailures().size());
        final VMResource resource = schedulingResult.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource();
        Assert.assertTrue("Failure type: " + resource, resource == VMResource.ResAllocs);
    }

    @Test
    public void testAddingNewResAllocs() throws Exception {
        final String nwGrpName="AbsentGrp";
        final TaskScheduler scheduler = getScheduler();
        final int numCores = (int)scheduler.getResAllocs().get(grp1).getCores();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, (double)numCores, numCores*1000.0, 1024.0, 1, 100);
        final List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(nwGrpName, 1.0, 1000.0, 100.0, 1));
        SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals("#failures: ", 1, schedulingResult.getFailures().size());
        final VMResource resource = schedulingResult.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource();
        Assert.assertTrue("Failure type: " + resource, resource == VMResource.ResAllocs);
        scheduler.addOrReplaceResAllocs(ResAllocsProvider.create(nwGrpName, 4, 4000, Double.MAX_VALUE, Double.MAX_VALUE));
        leases.clear();
        schedulingResult = scheduler.scheduleOnce(tasks, leases);
        int successes=0;
        for(Map.Entry<String, VMAssignmentResult> entry : schedulingResult.getResultMap().entrySet()) {
            for(TaskAssignmentResult r: entry.getValue().getTasksAssigned()) {
                if(r.isSuccessful())
                    successes++;
            }
        }
        Assert.assertEquals("Incorrect #success assignments: ", 1, successes);
        Assert.assertEquals("#failures: ", 0, schedulingResult.getFailures().size());
    }
}
