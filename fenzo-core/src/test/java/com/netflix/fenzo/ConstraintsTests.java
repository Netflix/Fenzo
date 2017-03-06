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
import com.netflix.fenzo.plugins.BalancedHostAttrConstraint;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;
import com.netflix.fenzo.plugins.UniqueHostAttrConstraint;
import org.junit.Assert;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class ConstraintsTests {

    private final static String zoneAttrName="Zone";
    private final int numZones=3;
    Map<String, Protos.Attribute> attributesA = new HashMap<>();
    Map<String, Protos.Attribute> attributesB = new HashMap<>();
    Map<String, Protos.Attribute> attributesC = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        Protos.Attribute attributeA = Protos.Attribute.newBuilder().setName(zoneAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue("zoneA")).build();
        attributesA.put(zoneAttrName, attributeA);
        Protos.Attribute attributeB = Protos.Attribute.newBuilder().setName(zoneAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue("zoneB")).build();
        attributesB.put(zoneAttrName, attributeB);
        Protos.Attribute attributeC = Protos.Attribute.newBuilder().setName(zoneAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue("zoneC")).build();
        attributesC.put(zoneAttrName, attributeC);
    }

    private TaskScheduler getTaskScheduler() {
        return getTaskScheduler(true);
    }
    private TaskScheduler getTaskScheduler(boolean useBinPacking) {
        TaskScheduler.Builder builder = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                        Assert.fail();
                    }
                })
                .withFitnessGoodEnoughFunction(new Func1<Double, Boolean>() {
                    @Override
                    public Boolean call(Double aDouble) {
                        return aDouble > 1.0;
                    }
                });
        if(useBinPacking)
            builder = builder
                    .withFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker);
        return builder.build();
    }

    @After
    public void tearDown() throws Exception {
    }

    // Test that hard constraint setup for one task per zone is honored by scheduling three tasks using three hosts.
    // All three tasks and three hosts are scheduled in one iteration. Each host could hold all three tasks if the constraint
    // isn't evaluated, which fail this test.
    @Test
    public void testHardConstraint1() throws Exception {
        Map<String, TaskRequest> taskMap = new HashMap<>();
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        ConstraintEvaluator zoneConstraint = new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        }, zoneAttrName);
        List<TaskRequest> tasks = getThreeTasks(taskMap, taskToCoTasksMap, zoneConstraint, null);
        List<VirtualMachineLease> leases = getThreeVMs();
        TaskScheduler taskScheduler = getTaskScheduler();
        SchedulingResult schedulingResult = taskScheduler.scheduleOnce(tasks, leases);
        Assert.assertEquals(numZones, schedulingResult.getResultMap().size());
    }

    // Test that a hard constraint setup for one task per zone is honored by scheduling one task per iteration. First,
    // test that without using the constraint three tasks land on the same host. Then, test with three other tasks with
    // the constraint and ensure they land on different hosts, one per zone.
    @Test
    public void testHardConstraint2() throws Exception {
        // first, make sure that when not using zone constraint, 3 tasks land on the same zone
        Set<String> zonesUsed = getZonesUsed(new HashMap<String, TaskRequest>(), new HashMap<String, Set<String>>(), null, null);
        Assert.assertEquals("Without zone constraints expecting 3 tasks to land on same zone", 1, zonesUsed.size());
        // now test with zone constraint and make sure they go on to one zone each
        Map<String, TaskRequest> taskMap = new HashMap<>();
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        ConstraintEvaluator zoneConstraint = new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        }, zoneAttrName);
        zonesUsed = getZonesUsed(taskMap, taskToCoTasksMap, zoneConstraint, null);
        Assert.assertEquals("With zone constraint, expecting 3 tasks to land on 3 zones", 3, zonesUsed.size());
    }

    // Test that using soft constraints also lands three tasks on three different hosts like in hard constraints case.
    @Test
    public void testSoftConstraint1() throws Exception {
        Map<String, TaskRequest> taskMap = new HashMap<>();
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        UniqueHostAttrConstraint zoneConstraint = new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        }, zoneAttrName);
        Set<String> zonesUsed = getZonesUsed(taskMap, taskToCoTasksMap, null, AsSoftConstraint.get(zoneConstraint));
        Assert.assertEquals(3, zonesUsed.size());
    }

    // test that when using a soft constraint, tasks get assigned even if constraint were to fail. We submit three tasks
    // with a soft constraint of wanting to land on different zones. But, provide only two hosts. All three tasks should
    // get assigned on the two hosts, not just one host.
    @Test
    public void testSoftConstraint2() throws Exception {
        Map<String, TaskRequest> taskMap = new HashMap<>();
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        UniqueHostAttrConstraint zoneConstraint = new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        }, zoneAttrName);
        List<TaskRequest> threeTasks = getThreeTasks(taskMap, taskToCoTasksMap, null, AsSoftConstraint.get(zoneConstraint));
        List<VirtualMachineLease> twoVMs = getThreeVMs().subList(0, 2);
        TaskScheduler taskScheduler = getTaskScheduler();
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(threeTasks, twoVMs).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(twoVMs.size(), resultMap.size());
        int tasksAssigned=0;
        Set<String> zonesUsed = new HashSet<>();
        for(VMAssignmentResult r: resultMap.values()) {
            Assert.assertEquals(1, r.getLeasesUsed().size());
            VirtualMachineLease lease = r.getLeasesUsed().get(0);
            zonesUsed.add(lease.getAttributeMap().get(zoneAttrName).getText().getValue());
            tasksAssigned += r.getTasksAssigned().size();
        }
        Assert.assertEquals(twoVMs.size(), zonesUsed.size());
        Assert.assertEquals(3, tasksAssigned);
    }

    @Test
    public void testBalancedHostAttrSoftConstraint() throws Exception {
        Map<String, TaskRequest> taskMap = new HashMap<>();
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        BalancedHostAttrConstraint constraint = new BalancedHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        }, zoneAttrName, 3);
        List<TaskRequest> sixTasks = getThreeTasks(taskMap, taskToCoTasksMap, null, constraint.asSoftConstraint());
        sixTasks.addAll(getThreeTasks(taskMap, taskToCoTasksMap, null, constraint.asSoftConstraint()));
        final List<VirtualMachineLease> threeVMs = getThreeVMs();
        final TaskScheduler taskScheduler = getTaskScheduler();
        final Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(sixTasks, threeVMs).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(3, resultMap.size());
        for(VMAssignmentResult r: resultMap.values()) {
            Assert.assertEquals(2, r.getTasksAssigned().size());
        }
    }

    @Test
    public void testBalancedHostAttrHardConstraint() throws Exception {
        Map<String, TaskRequest> taskMap = new HashMap<>();
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        BalancedHostAttrConstraint constraint = new BalancedHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        }, zoneAttrName, 3);
        List<TaskRequest> sixTasks = getThreeTasks(taskMap, taskToCoTasksMap, constraint, null);
        sixTasks.addAll(getThreeTasks(taskMap, taskToCoTasksMap, constraint, null));
        final List<VirtualMachineLease> threeVMs = getThreeVMs();
        final TaskScheduler taskScheduler = getTaskScheduler();
        final Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(sixTasks, threeVMs).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(3, resultMap.size());
        for(VMAssignmentResult r: resultMap.values()) {
            Assert.assertEquals(2, r.getTasksAssigned().size());
        }
    }

    // test that tasks that specify unique hosts land on separate hosts
    @Test
    public void testUniqueHostConstraint() throws Exception {
        final Map<String, Set<String>> taskToCoTasksMap = new HashMap<>();
        UniqueHostAttrConstraint uniqueHostConstraint = new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                return taskToCoTasksMap.get(s);
            }
        });
        List<TaskRequest> tasks = new ArrayList<>();
        // First, create 5 tasks without unique host constraint
        for(int i=0; i<5; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        TaskScheduler taskScheduler = getTaskScheduler();
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(5, 8, 8000, 1, 10);
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        leases.clear();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals("Tasks without unique host constraints should've landed on one host", 1, resultMap.size());
        int numAssigned=0;
        for(VMAssignmentResult r: resultMap.values()) {
            numAssigned += r.getTasksAssigned().size();
            double usedCpus=0.0;
            double usedMemory=0.0;
            List<Integer> usedPorts = new ArrayList<>();
            for(TaskAssignmentResult a: r.getTasksAssigned()) {
                taskScheduler.getTaskAssigner().call(a.getRequest(), r.getHostname());
                usedCpus += a.getRequest().getCPUs();
                usedMemory += a.getRequest().getMemory();
                usedPorts.addAll(a.getAssignedPorts());
            }
            leases.add(LeaseProvider.getConsumedLease(r.getLeasesUsed().get(0), usedCpus, usedMemory, usedPorts));
        }
        Assert.assertEquals("Tasks without unique host constraints should've gotten assigned", tasks.size(), numAssigned);
        // now test with tasks with unique host constraint
        tasks.clear();
        for(int i=0; i<5; i++)
            tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1, Arrays.asList(uniqueHostConstraint), null));
        for(TaskRequest r: tasks) {
            taskToCoTasksMap.put(r.getId(), new HashSet<String>());
            for(TaskRequest rr: tasks)
                if(!rr.getId().equals(r.getId()))
                    taskToCoTasksMap.get(r.getId()).add(rr.getId());
        }
        resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(tasks.size(), resultMap.size());
    }

    // tests that a task with exclusive host constraint gets assigned a host that has no other tasks on it
    @Test
    public void testExclusiveHostConstraint() throws Exception {
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, 8, 8000, 1, 10);
        TaskScheduler taskScheduler = getTaskScheduler();
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        VMAssignmentResult vmAssignmentResult = resultMap.values().iterator().next();
        String usedHostname = vmAssignmentResult.getHostname();
        Assert.assertEquals(1, vmAssignmentResult.getTasksAssigned().size());
        TaskAssignmentResult taskAssigned = vmAssignmentResult.getTasksAssigned().iterator().next();
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(vmAssignmentResult.getLeasesUsed().get(0),
                taskAssigned.getRequest().getCPUs(), taskAssigned.getRequest().getMemory(),
                taskAssigned.getAssignedPorts()));
        // now submit two tasks, one of them with exclusive host constraint. The one with the constraint should land
        // on a host that is not the usedHostname, and the other task should land on usedHostname
        tasks.clear();
        TaskRequest noCtask = TaskRequestProvider.getTaskRequest(1, 1000, 1);
        tasks.add(noCtask);
        TaskRequest yesCTask = TaskRequestProvider.getTaskRequest(1, 1000, 1, Arrays.asList(new ExclusiveHostConstraint()), null);
        tasks.add(yesCTask);
        resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertEquals(2, resultMap.size());
        for(VMAssignmentResult result: resultMap.values()) {
            Assert.assertEquals(1, result.getTasksAssigned().size());
            TaskAssignmentResult tResult = result.getTasksAssigned().iterator().next();
            if(tResult.getRequest().getId().equals(noCtask.getId()))
                Assert.assertEquals("Task with no constraint should've landed on the already used host", usedHostname, result.getHostname());
            else
                Assert.assertFalse(usedHostname.equals(result.getHostname()));
        }
    }

    // Test that a host that already has a task with an exclusive host constraint isn't picked for a new task
    @Test
    public void testExclusiveHostOnExistingTask() throws Exception {
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1, Arrays.asList(new ExclusiveHostConstraint()), null));
        List<VirtualMachineLease> leases = new ArrayList<>();
        leases.add(LeaseProvider.getLeaseOffer("hostA", 8, 8000, 1, 10));
        TaskScheduler taskScheduler = getTaskScheduler();
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        taskScheduler.getTaskAssigner().call(tasks.get(0), "hostA");
        tasks.clear();
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(resultMap.values().iterator().next()));
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals("New task shouldn't have been assigned a host with a task that has exclusiveHost constraint", 0, resultMap.size());
        leases.clear();
        leases.add(LeaseProvider.getLeaseOffer("hostB", 8, 8000, 1, 10));
        resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        Assert.assertEquals("hostB", resultMap.values().iterator().next().getHostname());
    }

    // Tests that a task gets assigned a host that has the zone attribute that the task asked for
    @Test
    public void testHostAttrValueConstraint() throws Exception {
        List<VirtualMachineLease> threeVMs = getThreeVMs();
        final String[] zones = new String[threeVMs.size()];
        int i=0;
        for(VirtualMachineLease l: threeVMs)
            zones[i++] = l.getAttributeMap().get(zoneAttrName).getText().getValue();
        // first submit two tasks and ensure they land on same machine, should be the case since we use bin packing
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        TaskScheduler taskScheduler = getTaskScheduler();
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(tasks, threeVMs).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        VMAssignmentResult onlyResult = resultMap.values().iterator().next();
        List<VirtualMachineLease> remainingLeases = Arrays.asList(LeaseProvider.getConsumedLease(onlyResult));
        String usedZone = onlyResult.getLeasesUsed().iterator().next().getAttributeMap().get(zoneAttrName).getText().getValue();
        final int useZoneIndex = usedZone.equals(zones[0])? 1 : 0; // pick a zone other than used
        HostAttrValueConstraint c = new HostAttrValueConstraint(zoneAttrName, new Func1<String, String>() {
            @Override
            public String call(String s) {
                return zones[useZoneIndex];
            }
        });
        tasks.clear();
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        // now add task with host attr value constraint
        TaskRequest tForPickedZone = TaskRequestProvider.getTaskRequest(1, 1000, 1, Arrays.asList(c), null);
        tasks.add(tForPickedZone);
        resultMap = taskScheduler.scheduleOnce(tasks, remainingLeases).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertTrue("Couldn't confirm test, all tasks landed on same machine", resultMap.size() > 1);
        boolean found=false;
        for(VMAssignmentResult result: resultMap.values()) {
            for(TaskAssignmentResult assigned: result.getTasksAssigned()) {
                if(assigned.getRequest().getId().equals(tForPickedZone.getId())) {
                    Assert.assertEquals(zones[useZoneIndex],
                            result.getLeasesUsed().iterator().next().getAttributeMap().get(zoneAttrName).getText().getValue());
                    found = true;
                }
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testHostnameValueConstraint() throws Exception {
        final List<VirtualMachineLease> threeVMs = getThreeVMs();
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        HostAttrValueConstraint c1 = new HostAttrValueConstraint(null, new Func1<String, String>() {
            @Override
            public String call(String s) {
                return threeVMs.get(0).hostname();
            }
        });
        HostAttrValueConstraint c2 = new HostAttrValueConstraint(null, new Func1<String, String>() {
            @Override
            public String call(String s) {
                return threeVMs.get(1).hostname();
            }
        });
        TaskRequest t1 = TaskRequestProvider.getTaskRequest(1, 1000, 1, Arrays.asList(c1), null);
        TaskRequest t2 = TaskRequestProvider.getTaskRequest(1, 1000, 1, Arrays.asList(c2), null);
        tasks.add(t1);
        tasks.add(t2);
        Map<String, VMAssignmentResult> resultMap = getTaskScheduler().scheduleOnce(tasks, threeVMs.subList(0,2)).getResultMap();
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(2, resultMap.size());
        for(VMAssignmentResult result: resultMap.values()) {
            for(TaskAssignmentResult r: result.getTasksAssigned()) {
                if(r.getRequest().getId().equals(t1.getId()))
                    Assert.assertEquals(threeVMs.get(0).hostname(), result.getHostname());
                else if(r.getRequest().getId().equals(t2.getId()))
                    Assert.assertEquals(threeVMs.get(1).hostname(), result.getHostname());
            }
        }
    }

    private Set<String> getZonesUsed(Map<String, TaskRequest> taskMap, Map<String, Set<String>> taskToCoTasksMap,
                                     ConstraintEvaluator hardConstraint, VMTaskFitnessCalculator softConstraint) {
        List<TaskRequest> tasks1 = getThreeTasks(taskMap, taskToCoTasksMap, hardConstraint, softConstraint);
        List<VirtualMachineLease> leases1 = getThreeVMs();
        TaskScheduler taskScheduler1 = getTaskScheduler();
        Set<String> zonesUsed1 = new HashSet<>();
        for(int i=0; i<tasks1.size(); i++) {
            Map<String, VMAssignmentResult> resultMap = taskScheduler1.scheduleOnce(tasks1.subList(i, i + 1), leases1).getResultMap();
            leases1.clear();
            Assert.assertNotNull(resultMap);
            Assert.assertEquals(1, resultMap.size());
            VMAssignmentResult result = resultMap.values().iterator().next();
            VirtualMachineLease lease = result.getLeasesUsed().get(0);
            zonesUsed1.add(lease.getAttributeMap().get(zoneAttrName).getText().getValue());
            Assert.assertEquals(1, result.getTasksAssigned().size());
            TaskAssignmentResult aResult = result.getTasksAssigned().iterator().next();
            TaskRequest request = aResult.getRequest();
            taskScheduler1.getTaskAssigner().call(request, lease.hostname());
            VirtualMachineLease unUsedLease = LeaseProvider.getConsumedLease(lease, request.getCPUs(), request.getMemory(), aResult.getAssignedPorts());
            leases1.add(unUsedLease);
        }
        return zonesUsed1;
    }

    private List<VirtualMachineLease> getThreeVMs() {
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        leases.add(LeaseProvider.getLeaseOffer("hostA", 4, 4000, ports, attributesA));
        leases.add(LeaseProvider.getLeaseOffer("hostB", 4, 4000, ports, attributesB));
        leases.add(LeaseProvider.getLeaseOffer("hostC", 4, 4000, ports, attributesC));
        return leases;
    }

    private List<TaskRequest> getThreeTasks(Map<String, TaskRequest> taskMap, Map<String, Set<String>> taskToCoTasksMap,
                                            ConstraintEvaluator hardConstraint, VMTaskFitnessCalculator softConstraint) {
        List<TaskRequest> tasks = new ArrayList<>(3);
        List<ConstraintEvaluator> constraintEvaluators = new ArrayList<>(1);
        if(hardConstraint!=null)
            constraintEvaluators.add(hardConstraint);
        List<VMTaskFitnessCalculator> softConstraints = new ArrayList<>();
        if(softConstraint!=null)
            softConstraints.add(softConstraint);
        for(int i=0; i<numZones; i++) {
            TaskRequest t = TaskRequestProvider.getTaskRequest(1, 1000, 1, constraintEvaluators, softConstraints);
            taskMap.put(t.getId(), t);
            tasks.add(t);
        }
        for(TaskRequest t: taskMap.values()) {
            Set<String> coTasks = new HashSet<>();
            taskToCoTasksMap.put(t.getId(), coTasks);
            for(TaskRequest i: taskMap.values())
                coTasks.add(i.getId());
        }
        return tasks;
    }

    @Test
    public void testExceptionInConstraints() throws Exception {
        final List<VirtualMachineLease> threeVMs = getThreeVMs();
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        HostAttrValueConstraint c1 = new HostAttrValueConstraint(null, new Func1<String, String>() {
            @Override
            public String call(String s) {
                throw new NullPointerException("Test exception");
            }
        });
        tasks.add(TaskRequestProvider.getTaskRequest(1, 100, 1));
        tasks.add(TaskRequestProvider.getTaskRequest(1, 1000, 1, Collections.singletonList(c1), null));
        tasks.add(TaskRequestProvider.getTaskRequest(1, 100, 1));
        try {
            final SchedulingResult result = getTaskScheduler().scheduleOnce(tasks, threeVMs);
            // expect to see 1 result for the first task before encountering exception with the 2nd task
            Assert.assertEquals(0, result.getResultMap().size());
            Assert.assertEquals(1, result.getExceptions().size());
        }
        catch (IllegalStateException e) {
            Assert.fail("Unexpected exception: " + e.getMessage());
        }
    }
}
