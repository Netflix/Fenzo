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
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import org.junit.Assert;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResourceSetsTests {

    private static final String resSetsAttrName = "res";
    static String hostAttrName = "MachineType";
    static final String hostAttrVal1="4coreServers";

    static Map<String, Protos.Attribute> getResSetsAttributesMap(String name, int x, int y) {
        Map<String, Protos.Attribute> attr = new HashMap<>();
        attr.put(
                resSetsAttrName,
                Protos.Attribute.newBuilder().setName(resSetsAttrName)
                        .setType(Protos.Value.Type.TEXT)
                        .setText(Protos.Value.Text.newBuilder().setValue(
                                PreferentialNamedConsumableResourceSet.attributeName + "-" + name + "-" + x + "-" + y
                        )).build()
        );
        attr.put(
                hostAttrName,
                Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1))
                .build()
        );
        return attr;
    }

    private TaskScheduler getTaskScheduler() {
        return getTaskScheduler(true);
    }
    private TaskScheduler getTaskScheduler(boolean useBinPacking) {
        return getTaskSchedulerWithAutoscale(useBinPacking, null);
    }
    private TaskScheduler getTaskSchedulerWithAutoscale(boolean useBinPacking, Action1<AutoScaleAction> callback, AutoScaleRule... rules) {
        final TaskScheduler.Builder builder = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                    }
                })
                .withAutoScaleByAttributeName(hostAttrName)
                .withFitnessGoodEnoughFunction(new Func1<Double, Boolean>() {
                    @Override
                    public Boolean call(Double aDouble) {
                        return aDouble > 1.0;
                    }
                });
        if(callback!=null && rules!=null) {
            builder.withAutoScalerCallback(callback);
            for (AutoScaleRule r : rules)
                builder.withAutoScaleRule(r);
        }
        if(useBinPacking)
            builder
                    .withFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker);
        return builder.build();
    }

    // Create 4 tasks to be launched on 1 host which has 3 resource sets. Two tasks ask for one value for the
    // resourceSet and the other two tasks ask for another value. Confirm that exactly two of the three resource sets
    // are used.
    @Test
    public void testSimpleResourceSetAllocation() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=8;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 1);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg2", 1, 1);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        // add two more tasks with similar resource requests
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        final SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertTrue(result.getResultMap().size() == 1);
        final VMAssignmentResult assignmentResult = result.getResultMap().values().iterator().next();
        Set<Integer> uniqueResSetIndeces = new HashSet<>();
        for(TaskAssignmentResult r: assignmentResult.getTasksAssigned()) {
            final List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = r.getrSets();
            Assert.assertTrue(consumeResults != null);
            System.out.println("Task " + r.getRequest().getId() + ": index=" + consumeResults.get(0).getIndex());
            uniqueResSetIndeces.add(consumeResults.get(0).getIndex());
        }
        Assert.assertTrue(uniqueResSetIndeces.size() == 2);
    }

    // Create a host that has 3 resource sets. Create three tasks with different resource values so each get assigned
    // one of the 3 resource sets - so all 3 resource sets are now assigned. Although, each will have additional
    // subResources available. Submit two new tasks - one with a resource value of one of the three already assigned,
    // and another task with a new fourth resource value. Confirm that the first of these two tasks succeeds in
    // assignment but the second one fails.
    @Test
    public void testResSetAllocationFailure() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=8;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 1);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg2", 1, 1);
        TaskRequest.NamedResourceSetRequest sr3 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg3", 1, 1);
        TaskRequest.NamedResourceSetRequest sr4 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg4", 1, 1);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr3.getResName(), sr3)));
        // task 4 same as task 1
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.25, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        // task 5 uses sg4
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.25, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr4)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        final SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertTrue(result.getResultMap().size() == 1);
        final VMAssignmentResult assignmentResult = result.getResultMap().values().iterator().next();
        Set<Integer> uniqueResSetIndeces = new HashSet<>();
        int tasksAssigned=0;
        for(TaskAssignmentResult r: assignmentResult.getTasksAssigned()) {
            tasksAssigned++;
            final List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = r.getrSets();
            Assert.assertTrue(consumeResults != null);
            //System.out.println("Task " + r.getRequest().getId() + ": index=" + consumeResults.get(0).getIndex());
            uniqueResSetIndeces.add(consumeResults.get(0).getIndex());
        }
        Assert.assertTrue(uniqueResSetIndeces.size() == 3);
        Assert.assertTrue(tasksAssigned == 4);
        final Map<TaskRequest, List<TaskAssignmentResult>> failures = result.getFailures();
        Assert.assertTrue(failures != null && failures.size() == 1);
        final List<TaskAssignmentResult> fail = failures.values().iterator().next();
        Assert.assertTrue(fail.size() == 1);
        final List<AssignmentFailure> failDetails = fail.iterator().next().getFailures();
        Assert.assertTrue(failDetails!=null && failDetails.size()==1);
        Assert.assertTrue(failDetails.iterator().next().getResource() == VMResource.ResourceSet);
        //System.out.println(failDetails.iterator().next());
    }

    // Create a host that has 3 resource sets, each with 2 sub-resources. Submit a task that uses 1 rSet with
    // 2 subResources. Then submit another similar job, confirm that it goes to a different resourceSet. Same again
    // with a 3rd job. Then, submit a 4th job and ensure that although its resource requests for cpu/memory would fit,
    // it fails since there are no more resource sets available
    @Test
    public void testResSetAllocFillupSubRes() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 2);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        final SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertTrue(result.getResultMap().size() == 1);
        final VMAssignmentResult assignmentResult = result.getResultMap().values().iterator().next();
        Set<Integer> uniqueResSetIndeces = new HashSet<>();
        int tasksAssigned=0;
        for(TaskAssignmentResult r: assignmentResult.getTasksAssigned()) {
            tasksAssigned++;
            final List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = r.getrSets();
            Assert.assertTrue(consumeResults != null);
            //System.out.println("Task " + r.getRequest().getId() + ": index=" + consumeResults.get(0).getIndex());
            uniqueResSetIndeces.add(consumeResults.get(0).getIndex());
        }
        Assert.assertTrue(uniqueResSetIndeces.size() == 3);
        Assert.assertTrue(tasksAssigned == 3);
        final Map<TaskRequest, List<TaskAssignmentResult>> failures = result.getFailures();
        Assert.assertTrue(failures != null && failures.size() == 1);
        final List<TaskAssignmentResult> fail = failures.values().iterator().next();
        Assert.assertTrue(fail.size() == 1);
        final List<AssignmentFailure> failDetails = fail.iterator().next().getFailures();
        Assert.assertTrue(failDetails!=null && failDetails.size()==1);
        Assert.assertTrue(failDetails.iterator().next().getResource() == VMResource.ResourceSet);
    }

    // Create a host with 3 resource sets, each with 2 sub-resources. Submit two tasks that use up two of the 3 sets.
    // That is, two tasks, each with different value for the resource set name, rs1 and rs2. Then, submit multiple tasks
    // each with a request for a 3rd value for the resource name, rs3, but, asking for 0 sub-resources. Ensure that we
    // are able to launch N of these, where N > 2 (sub-resources), at least until other resources on the host fill up
    // (cpu/memory). And that the tasks asking for resource set value rs3 land on a different set than those asking
    // for rs1 and rs2.
    @Test
    public void testResSetAllocFillWith0SubresRequests() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 1);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg2", 1, 1);
        TaskRequest.NamedResourceSetRequest sr3 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg3", 1, 0);
        // create two tasks, one asking for sr1 and another asking sr2
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        // create 10 tasks asking for sr3
        final int N=20; // #tasks for sr3
        for(int i=0; i<N; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(
                    "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr3.getResName(), sr3)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        final SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertTrue(result.getResultMap().size() == 1);
        Assert.assertEquals(0, result.getFailures().size());
        final VMAssignmentResult assignmentResult = result.getResultMap().values().iterator().next();
        final Map<String, Integer> counts = new HashMap<>();
        for(TaskAssignmentResult r: assignmentResult.getTasksAssigned()) {
            final String value = r.getRequest().getCustomNamedResources().get("ENIs").getResValue();
            if(counts.get(value) == null)
                counts.put(value, 1);
            else
                counts.put(value, counts.get(value)+1);
        }
        List<Integer> li = new ArrayList<>(counts.values());
        Collections.sort(li);
        Assert.assertEquals(1, (int) li.get(0));
        Assert.assertEquals(1, (int) li.get(1));
        Assert.assertEquals(N, (int) li.get(2));
    }

    // Create a host with 3 resource sets, each with 2 sub-resources. Submit these types of jobs:
    //   - Job type 1: asks for resource set sr1 with 1 sub-resource
    //   - Job type 2: asks for resource set sr2 with 1 sub-resource
    //   - Job type 3: does not ask for any resource set
    //   - Job type 4: asks for resource set sr3 with 1 sub-resource
    // Submit 1 job each of type 1 and type 2. Then submit 2 jobs of type 3.
    // Ensure that all of them get allocated. Even though the type 3 job doesn't ask for a resource set, it is supposed
    // to get assigned a "catch-all" resource set.
    // Submit another job of type 4 and ensure that it does not get assigned.
    @Test
    public void testNoResSetReqJob() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 1);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg2", 1, 1);
        TaskRequest.NamedResourceSetRequest sr3 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg3", 1, 1);
        // create two tasks, one asking for sr1 and another asking sr2
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertTrue(result.getResultMap().size() == 1);
        Assert.assertEquals(0, result.getFailures().size());
        final VMAssignmentResult assignmentResult = result.getResultMap().values().iterator().next();
        Set<String> uniqueSRs = new HashSet<>();
        for(TaskAssignmentResult r: assignmentResult.getTasksAssigned()) {
            uniqueSRs.add(r.getRequest().getCustomNamedResources().values().iterator().next().getResValue());
            taskScheduler.getTaskAssigner().call(r.getRequest(), "hostA");
        }
        Assert.assertEquals(2, uniqueSRs.size());
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next()));
        tasks.clear();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, null));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr3.getResName(), sr3)));
        result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertTrue(result.getResultMap().size() == 1);
        Assert.assertEquals(1, result.getResultMap().values().iterator().next().getTasksAssigned().size());
        Assert.assertTrue(result.getResultMap().values().iterator().next().getTasksAssigned().iterator().next()
                .getRequest().getCustomNamedResources() == null);
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(VMResource.ResourceSet,
                result.getFailures().values().iterator().next().get(0).getFailures().get(0).getResource());
    }

    // Test that an initialization of scheduler with tasks assigned from before does the right thing for next scheduling
    // iteration. Create a host with 3 resource sets, each with 2 sub-resources. Initialize scheduler with three tasks
    // already assigned from before. Each task asks for 1 sub-resource. Task 1 requests resource set sr1 and was assigned
    // on set 0. Task 2 requests resource set sr1 and was assigned on set 1. Task 3 requests resource set sr2 and was
    // assigned on set 2.
    // Now schedule 2 tasks, one asking for sr1 and another for sr3. The first task should get assigned and the second
    // one should not.
    // Then schedule another 2 tasks, one asking for sr2 and another asking sr3. The first task should get assigned
    // and the second task should not.
    @Test
    public void testInitingSchedulerWithPreviousTasks() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 1);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg2", 1, 1);
        TaskRequest.NamedResourceSetRequest sr3 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg3", 1, 1);
        // create three tasks, two asking for sr1 and another one asking sr2
        final TaskRequest task1 = TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1));
        final TaskRequest.AssignedResources ar1 = new TaskRequest.AssignedResources();
        ar1.setConsumedNamedResources(Collections.singletonList(
                new PreferentialNamedConsumableResourceSet.ConsumeResult(0, "ENIs", "sg1", 1.0)
        ));
        task1.setAssignedResources(ar1);
        final TaskRequest task2 = TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1));
        final TaskRequest.AssignedResources ar2 = new TaskRequest.AssignedResources();
        ar2.setConsumedNamedResources(Collections.singletonList(
                new PreferentialNamedConsumableResourceSet.ConsumeResult(1, "ENIs", "sg1", 1.0)
        ));
        task2.setAssignedResources(ar2);
        final TaskRequest task3 = TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2));
        final TaskRequest.AssignedResources ar3 = new TaskRequest.AssignedResources();
        ar3.setConsumedNamedResources(Collections.singletonList(
                new PreferentialNamedConsumableResourceSet.ConsumeResult(2, "ENIs", "sg2", 1.0)
        ));
        task3.setAssignedResources(ar3);
        final TaskScheduler taskScheduler = getTaskScheduler();
        taskScheduler.getTaskAssigner().call(task1, "hostA");
        taskScheduler.getTaskAssigner().call(task2, "hostA");
        taskScheduler.getTaskAssigner().call(task3, "hostA");
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr3.getResName(), sr3)));
        SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(1, result.getResultMap().values().iterator().next().getTasksAssigned().size());
        TaskAssignmentResult assignmentResult =
                result.getResultMap().values().iterator().next().getTasksAssigned().iterator().next();
        Assert.assertEquals("sg1", assignmentResult.getRequest().getCustomNamedResources().values().iterator().next().getResValue());
        taskScheduler.getTaskAssigner().call(assignmentResult.getRequest(), "hostA");
        tasks.clear();
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next()));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr3.getResName(), sr3)));
        result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(1, result.getResultMap().values().iterator().next().getTasksAssigned().size());
        assignmentResult =
                result.getResultMap().values().iterator().next().getTasksAssigned().iterator().next();
        Assert.assertEquals("sg2", assignmentResult.getRequest().getCustomNamedResources().values().iterator().next().getResValue());
    }

    // Test that when resource sets are unavailable on one host, they are assigned on another host
    // Set up hostA and hostB each with 3 resource sets, each with 2 sub-resources. Submit 4 tasks, each
    // asking for the same resource set, sr1, with 2 sub-resources. Confirm that one of the hosts gets 3 tasks and
    // the other host gets 1 task
    @Test
    public void testTwoHostAssignment() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostB", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 2);
        List<TaskRequest> tasks = new ArrayList<>();
        for(int i=0; i<4; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(
                    "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        final SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(2, result.getResultMap().size());
        Assert.assertEquals(0, result.getFailures().size());
        Map<String, Integer> counts = new HashMap<>();
        for(VMAssignmentResult r: result.getResultMap().values()) {
            counts.put(r.getHostname(), r.getTasksAssigned().size());
        }
        List<Integer> sortedCounts = new ArrayList<>(counts.values());
        Collections.sort(sortedCounts);
        Assert.assertEquals(1, (int)sortedCounts.get(0));
        Assert.assertEquals(3, (int)sortedCounts.get(1));
    }

    // Test that on a host that has all of its resource sets fully assigned, when all tasks assigned to a resource set
    // complete, that resource set is usable for other tasks of a different resource set name.
    @Test
    public void testReAssignment() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, numIPsPerEni);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg2", 1, numIPsPerEni);
        List<TaskRequest> tasks = new ArrayList<>();
        for(int i=0; i<numENIs; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(
                    "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final TaskScheduler taskScheduler = getTaskScheduler();
        SchedulingResult result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
        int tasksAssigned=0;
        String taskId=null;
        for(TaskAssignmentResult r: result.getResultMap().values().iterator().next().getTasksAssigned()) {
            tasksAssigned++;
            taskScheduler.getTaskAssigner().call(r.getRequest(), r.getHostname());
            taskId = r.getTaskId();
        }
        Assert.assertEquals(tasks.size(), tasksAssigned);
        tasks.clear();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next()));
        result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(0, result.getResultMap().size());
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(VMResource.ResourceSet,
                result.getFailures().values().iterator().next().iterator().next().getFailures().iterator().next().getResource());
        taskScheduler.getTaskUnAssigner().call(taskId, "hostA");
        leases.clear();
        result = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
    }

    // Test that when we run out of choices for placement due to resource sets, that a scale up is triggered.
    @Test
    public void testScaleUp() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostB", numCores, numCores * 1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, numIPsPerEni);
        List<TaskRequest> tasks = new ArrayList<>();
        for(int i=0; i<numENIs; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(
                    "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final int cooldown=2;
        final AtomicBoolean gotScaleupCallback = new AtomicBoolean(false);
        final TaskScheduler scheduler = getTaskSchedulerWithAutoscale(true,
                new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        gotScaleupCallback.set(true);
                    }
                },
                AutoScaleRuleProvider.createRule(hostAttrVal1, 1, 1, cooldown, 0, 0));
        SchedulingResult result=null;
        boolean first=true;
        for(int i=0; i<cooldown+1; i++) {
            result = scheduler.scheduleOnce(tasks, leases);
            if(first) {
                first = false;
                tasks.clear();
                leases.clear();
                Assert.assertEquals(1, result.getResultMap().size());
                Assert.assertEquals(0, result.getFailures().size());
            }
            Thread.sleep(1000);
        }
        Assert.assertFalse("Unexpected to get scale up callback", gotScaleupCallback.get());
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        first=true;
        for(int i=0; i<cooldown+1; i++) {
            result = scheduler.scheduleOnce(tasks, leases);
            if(first) {
                first = false;
                tasks.clear();
                leases.clear();
                Assert.assertEquals(1, result.getResultMap().size());
                Assert.assertEquals(0, result.getFailures().size());
            }
            Thread.sleep(1000);
        }
        Assert.assertTrue("Did not get scale up callback", gotScaleupCallback.get());
    }

    // Test that expiring a lease does not affect the available resource sets and its current usage counts
    @Test
    public void testLeaseExpiry() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, numIPsPerEni);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final TaskScheduler scheduler = getTaskScheduler();
        SchedulingResult result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(0, result.getFailures().size());
        for(TaskAssignmentResult r: result.getResultMap().values().iterator().next().getTasksAssigned()) {
            scheduler.getTaskAssigner().call(r.getRequest(), r.getHostname());
        }
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next()));
        tasks.clear();
        result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(0, result.getResultMap().size());
        Assert.assertEquals(0, result.getFailures().size());
        scheduler.expireAllLeases(leases.get(0).hostname());
        leases.clear();
        tasks.clear();
        //scheduler.scheduleOnce(tasks, leases);
        // now add new lease for the same hostname
        leases.clear();
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores * 1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        // create two new tasks; only one of them should get assigned
        tasks.clear();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(VMResource.ResourceSet,
                result.getFailures().values().iterator().next().iterator().next().getFailures().iterator().next().getResource());
    }

    // Test that when lease expires and there are no tasks running, the resource set is cleared. A new lease added
    // should recreate the resource set. Verify this by changing the resource set available before and after the lease
    // expiry.
    @Test
    public void testLeaseExpiryClearsResourceSets() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, numIPsPerEni);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("NewENIs", "sg2", 1, numIPsPerEni);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        List<TaskRequest> initialTasks = new ArrayList<>(tasks);
        final TaskScheduler scheduler = getTaskScheduler();
        SchedulingResult result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(0, result.getFailures().size());
        for(TaskAssignmentResult r: result.getResultMap().values().iterator().next().getTasksAssigned()) {
            scheduler.getTaskAssigner().call(r.getRequest(), r.getHostname());
        }
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next()));
        tasks.clear();
        result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(0, result.getResultMap().size());
        Assert.assertEquals(0, result.getFailures().size());
        Thread.sleep(500);
        // unassign tasks
        for(TaskRequest r: initialTasks)
            scheduler.getTaskUnAssigner().call(r.getId(), "hostA");
        scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), Collections.<VirtualMachineLease>emptyList());
        scheduler.expireAllLeases(leases.get(0).hostname());
        scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), Collections.<VirtualMachineLease>emptyList());
        // now submit 3 tasks of different resource set request and ensure all 3 get assigned
        leases.clear();
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores * 1000, ports, getResSetsAttributesMap("NewENIs", numENIs, numIPsPerEni)));
        tasks.clear();
        for(int i=0; i<numENIs-1; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(
                    "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr2.getResName(), sr2)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(numENIs-1, result.getResultMap().values().iterator().next().getTasksAssigned().size());
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals("ENIs",
                result.getFailures().keySet().iterator().next().getCustomNamedResources().keySet().iterator().next());
    }

    // Test that resource status represents the usage of resource sets correctly
    @Test
    public void testResourceStatusValues() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, getResSetsAttributesMap("ENIs", numENIs, numIPsPerEni)));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, numIPsPerEni);
        TaskRequest.NamedResourceSetRequest sr2 = new TaskRequest.NamedResourceSetRequest("NewENIs", "sg2", 1, numIPsPerEni);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final TaskScheduler scheduler = getTaskScheduler();
        SchedulingResult result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(1, result.getResultMap().size());
        Assert.assertEquals(0, result.getFailures().size());
        for(TaskAssignmentResult r: result.getResultMap().values().iterator().next().getTasksAssigned()) {
            scheduler.getTaskAssigner().call(r.getRequest(), r.getHostname());
        }
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next()));
        tasks.clear();
        scheduler.scheduleOnce(tasks, leases);
        final Map<VMResource, Double[]> usage = scheduler.getResourceStatus().get("hostA");
        final Double[] cpuUsg = usage.get(VMResource.CPU);
        Assert.assertTrue(0.2 == cpuUsg[0]);
        Assert.assertTrue(numCores - 0.2 == cpuUsg[1]);
        final Double[] rSetUsg = usage.get(VMResource.ResourceSet);
        Assert.assertTrue(2.0 == rSetUsg[0]);
        Assert.assertTrue(1.0 == rSetUsg[1]);
    }

    // Test that a task asking for a resource set that doesn't exist does not get assigned
    @Test
    public void testNonExistentResourceSetRequest() throws Exception {
        int numCores=4;
        int numENIs=numCores-1;
        int numIPsPerEni=2;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer(
                "hostA", numCores, numCores*1000, ports, null));
        TaskRequest.NamedResourceSetRequest sr1 = new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, numIPsPerEni);
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(
                "grp", 0.1, 100, 0, 0, 0, null, null, Collections.singletonMap(sr1.getResName(), sr1)));
        final TaskScheduler scheduler = getTaskScheduler();
        SchedulingResult result = scheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(0, result.getResultMap().size());
        Assert.assertEquals(1, result.getFailures().size());
        Assert.assertEquals(VMResource.ResourceSet,
                result.getFailures().values().iterator().next().get(0).getFailures().iterator().next().getResource());
    }
}
