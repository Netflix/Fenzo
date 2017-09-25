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

import com.netflix.fenzo.functions.Action0;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.queues.*;
import com.netflix.fenzo.queues.tiered.QueuableTaskProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TaskSchedulingServiceTest {

    private final QAttributes tier1bktA = new QAttributes.QAttributesAdaptor(0, "A");
    private final QAttributes tier1bktB = new QAttributes.QAttributesAdaptor(0, "B");
    private final QAttributes tier1bktC = new QAttributes.QAttributesAdaptor(0, "C");
    private final QAttributes tier1bktD1 = new QAttributes.QAttributesAdaptor(1, "D1");

    private TaskSchedulingService getSchedulingService(TaskQueue queue, TaskScheduler scheduler, long loopMillis,
                                                       Action1<SchedulingResult> resultCallback) {
        return getSchedulingService(queue, scheduler, loopMillis, loopMillis, resultCallback);
    }

    private TaskSchedulingService getSchedulingService(TaskQueue queue, TaskScheduler scheduler, long loopMillis,
                                                       long maxDelayMillis, Action1<SchedulingResult> resultCallback) {
        return new TaskSchedulingService.Builder()
                .withTaskQueue(queue)
                .withLoopIntervalMillis(loopMillis)
                .withMaxDelayMillis(maxDelayMillis)
                .withPreSchedulingLoopHook(new Action0() {
                    @Override
                    public void call() {
                        //System.out.println("Pre-scheduling hook");
                    }
                })
                .withSchedulingResultCallback(resultCallback)
                .withTaskScheduler(scheduler)
                .build();
    }

    public TaskScheduler getScheduler() {
        return new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                    }
                })
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker)
                .build();
    }

    @Test
    public void testOneTaskAssignment() throws Exception {
        testOneTaskInternal(
                QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(2, 2000, 1)),
                () -> {}
        );
    }

    private void testOneTaskInternal(QueuableTask queuableTask, Action0 action) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                //System.out.println("Got scheduling result with " + schedulingResult.getResultMap().size() + " results");
                if (schedulingResult.getResultMap().size() > 0) {
                    //System.out.println("Assignment on host " + schedulingResult.getResultMap().values().iterator().next().getHostname());
                    latch.countDown();
                    scheduler.shutdown();
                }
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, 1000L, resultCallback);
        schedulingService.start();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        schedulingService.addLeases(
                Collections.singletonList(LeaseProvider.getLeaseOffer(
                        "hostA", 4, 4000, ports,
                        ResourceSetsTests.getResSetsAttributesMap("ENIs", 2, 2))));
        queue.queueTask(queuableTask);
        if (!latch.await(20000, TimeUnit.MILLISECONDS))
            Assert.fail("Did not assign resources in time");
        if (action != null)
            action.call();
    }

    @Test
    public void testOneTaskWithResourceSet() throws Exception {
        TaskRequest.NamedResourceSetRequest sr1 =
                new TaskRequest.NamedResourceSetRequest("ENIs", "sg1", 1, 1);
        final QueuableTask task = QueuableTaskProvider.wrapTask(
                tier1bktA,
                TaskRequestProvider.getTaskRequest("grp", 1, 100, 0, 0, 0,
                        null, null, Collections.singletonMap(sr1.getResName(), sr1))
        );
        testOneTaskInternal(
                task,
                () -> {
                    final TaskRequest.AssignedResources assignedResources = task.getAssignedResources();
                    Assert.assertNotNull(assignedResources);
                    final List<PreferentialNamedConsumableResourceSet.ConsumeResult> cnrs = assignedResources.getConsumedNamedResources();
                    Assert.assertNotNull(cnrs);
                    Assert.assertEquals(1, cnrs.size());
                    Assert.assertEquals(sr1.getResValue(), cnrs.get(0).getResName());
                }
        );
    }

    @Test
    public void testMultipleTaskAssignments() throws Exception {
        int numTasks = 4;
        long loopMillis=100;
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final CountDownLatch latch = new CountDownLatch(numTasks);
        final TaskScheduler scheduler = getScheduler();
        final AtomicReference<TaskSchedulingService> ref = new AtomicReference<>();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                //System.out.println("Got scheduling result with " + schedulingResult.getResultMap().size() + " results");
                if (!schedulingResult.getExceptions().isEmpty()) {
                    Assert.fail(schedulingResult.getExceptions().get(0).getMessage());
                }
                else if (schedulingResult.getResultMap().size() > 0) {
                    final VMAssignmentResult vmAssignmentResult = schedulingResult.getResultMap().values().iterator().next();
//                    System.out.println("Assignment on host " + vmAssignmentResult.getHostname() +
//                            " with " + vmAssignmentResult.getTasksAssigned().size() + " tasks"
//                    );
                    for (TaskAssignmentResult r: vmAssignmentResult.getTasksAssigned()) {
                        latch.countDown();
                    }
                    ref.get().addLeases(
                            Collections.singletonList(LeaseProvider.getConsumedLease(vmAssignmentResult))
                    );
                }
                else {
                    final Map<TaskRequest, List<TaskAssignmentResult>> failures = schedulingResult.getFailures();
                    if (!failures.isEmpty()) {
                        Assert.fail(failures.values().iterator().next().iterator().next().toString());
                    }
                }
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, loopMillis, resultCallback);
        ref.set(schedulingService);
        schedulingService.start();
        schedulingService.addLeases(LeaseProvider.getLeases(1, 4, 4000, 1, 10));
        for (int i=0; i<numTasks; i++) {
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 1000, 1)));
            Thread.sleep(loopMillis); // simulate that tasks are added across different scheduling iterations
        }
        if (!latch.await(loopMillis * (numTasks + 2), TimeUnit.MILLISECONDS))
            Assert.fail(latch.getCount() + " of " + numTasks + " not scheduled within time");
    }

    // Test that tasks are assigned in the order based on current usage among multiple buckets within a tier
    @Test
    public void testOrderedAssignments() throws Exception {
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        final BlockingQueue<QueuableTask> assignmentResults = new LinkedBlockingQueue<>();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
                if (!resultMap.isEmpty()) {
                    for (VMAssignmentResult r: resultMap.values()) {
                        for (TaskAssignmentResult t: r.getTasksAssigned()) {
                            assignmentResults.offer((QueuableTask)t.getRequest());
                            //System.out.println("*******             Assignment for task " + t.getTaskId());
                        }
                    }
                }
//                final Map<TaskRequest, List<TaskAssignmentResult>> failures = schedulingResult.getFailures();
//                if (!failures.isEmpty()) {
//                    for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry: failures.entrySet()) {
//                        System.out.println("******                failures for task " + entry.getKey().getId());
//                    }
//                }
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, 50L, resultCallback);
        // First, fill 4 VMs, each with 8 cores, with A using 15 cores, B using 6 cores, and C using 11 cores, with
        // memory used in the same ratios
        for (int i=0; i<15; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        for (int i=0; i<6; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        for (int i=0; i<11; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktC, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        schedulingService.start();
        schedulingService.addLeases(LeaseProvider.getLeases(4, 8, 8000, 1, 1000));
        int numTasks = 32;
        while (numTasks > 0) {
            final QueuableTask task = assignmentResults.poll(2000, TimeUnit.MILLISECONDS);
            if (task == null)
                Assert.fail("Time out waiting for task to get assigned");
            else {
                numTasks--;
            }
        }
        // Now submit one task for each of A, B, and C, and create one offer that will only fit one of the tasks. Ensure
        // that the only task assigned is from B.
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(4, 40, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(4, 40, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktC, TaskRequestProvider.getTaskRequest(4, 40, 1)));
        schedulingService.addLeases(LeaseProvider.getLeases(1, 4, 4000, 1, 100));
        QueuableTask task = assignmentResults.poll(1000, TimeUnit.MILLISECONDS);
        if (task == null)
            Assert.fail("Time out waiting for just one task to get assigned");
        Assert.assertEquals(tier1bktB.getBucketName(), task.getQAttributes().getBucketName());
        // queueTask another task for B and make sure it gets launched ahead of A and C after adding one more offer
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(4, 40, 1)));
        schedulingService.addLeases(LeaseProvider.getLeases(1, 4, 4000, 1, 100));
        task = assignmentResults.poll(1000, TimeUnit.MILLISECONDS);
        if (task == null)
            Assert.fail("Time out waiting for just one task to get assigned");
        Assert.assertEquals(tier1bktB.getBucketName(), task.getQAttributes().getBucketName());
        // now add another offer and ensure task from C gets launched
        schedulingService.addLeases(LeaseProvider.getLeases(1, 4, 4000, 1, 100));
        task = assignmentResults.poll(1000, TimeUnit.MILLISECONDS);
        if (task == null)
            Assert.fail("Time out waiting for just one task to get assigned");
        Assert.assertEquals(tier1bktC.getBucketName(), task.getQAttributes().getBucketName());
        // a final offer and the task from A should get launched
        schedulingService.addLeases(LeaseProvider.getLeases(1, 4, 4000, 1, 100));
        task = assignmentResults.poll(1000, TimeUnit.MILLISECONDS);
        if (task == null)
            Assert.fail("Time out waiting for just one task to get assigned");
        Assert.assertEquals(tier1bktA.getBucketName(), task.getQAttributes().getBucketName());
    }

    @Test
    public void testMultiTierAllocation() throws Exception {
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        final BlockingQueue<QueuableTask> assignmentResults = new LinkedBlockingQueue<>();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
                if (!resultMap.isEmpty()) {
                    for (VMAssignmentResult r: resultMap.values()) {
                        for (TaskAssignmentResult t: r.getTasksAssigned()) {
                            assignmentResults.offer((QueuableTask)t.getRequest());
                            //System.out.println("*******             Assignment for task " + t.getTaskId());
                        }
                    }
                }
//                final Map<TaskRequest, List<TaskAssignmentResult>> failures = schedulingResult.getFailures();
//                if (!failures.isEmpty()) {
//                    for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry: failures.entrySet()) {
//                        System.out.println("******                failures for task " + entry.getKey().getId());
//                    }
//                }
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, 50L, resultCallback);
        // fill 4 hosts with tasks from A (tier 0) and tasks from D1 (tier 1)
        for (int i=0; i<20; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        for (int i=0; i<12; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktD1, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        schedulingService.start();
        schedulingService.addLeases(LeaseProvider.getLeases(4, 8, 8000, 1, 1000));
        int numTasks = 32;
        while (numTasks > 0) {
            final QueuableTask task = assignmentResults.poll(2000, TimeUnit.MILLISECONDS);
            if (task == null)
                Assert.fail("Time out waiting for task to get assigned");
            else {
                numTasks--;
            }
        }
        // now submit a task from A that will only fill part of next offer, and a few tasks from D1 each of which
        // can fill the entire offer. Ensure that only task from A gets launched
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        for (int i=0; i<3; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktD1, TaskRequestProvider.getTaskRequest(4, 40, 1)));
        schedulingService.addLeases(LeaseProvider.getLeases(1, 4, 4000, 1, 1000));
        final QueuableTask task = assignmentResults.poll(1000, TimeUnit.MILLISECONDS);
        Assert.assertNotNull("Time out waiting for just one task to get assigned", task);
        Assert.assertEquals(tier1bktA.getBucketName(), task.getQAttributes().getBucketName());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Map<TaskQueue.TaskState, Collection<QueuableTask>>> ref = new AtomicReference<>();
        schedulingService.requestAllTasks(new Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>>() {
            @Override
            public void call(Map<TaskQueue.TaskState, Collection<QueuableTask>> stateCollectionMap) {
                //System.out.println("**************** Got tasks collection");
                final Collection<QueuableTask> tasks = stateCollectionMap.get(TaskQueue.TaskState.QUEUED);
                //System.out.println("********* size=" + tasks.size());
//                if (!tasks.isEmpty())
//                    System.out.println("******** bucket: " + tasks.iterator().next().getQAttributes().getBucketName());
                ref.set(stateCollectionMap);
                latch.countDown();
            }
        });
        if (!latch.await(1000, TimeUnit.MILLISECONDS))
            Assert.fail("Time out waiting for tasks collection");
        final Map<TaskQueue.TaskState, Collection<QueuableTask>> map = ref.get();
        Assert.assertNotNull(map);
        Assert.assertNotNull(map.get(TaskQueue.TaskState.QUEUED));
        Assert.assertEquals(tier1bktD1.getBucketName(), map.get(TaskQueue.TaskState.QUEUED).iterator().next().getQAttributes().getBucketName());
    }

    // test that dominant resource share works for ordering of buckets - test by having equal resource usage among two
    // buckets A and B, then let A use more CPUs and B use more Memory.
    @Test
    public void testMultiResAllocation() throws Exception {
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        final BlockingQueue<QueuableTask> assignmentResults = new LinkedBlockingQueue<>();
        final AtomicReference<TaskSchedulingService> ref = new AtomicReference<>();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
                if (!resultMap.isEmpty()) {
                    for (VMAssignmentResult r: resultMap.values()) {
                        for (TaskAssignmentResult t: r.getTasksAssigned()) {
                            assignmentResults.offer((QueuableTask)t.getRequest());
                        }
                        ref.get().addLeases(Collections.singletonList(LeaseProvider.getConsumedLease(r)));
                    }
                }
//                final Map<TaskRequest, List<TaskAssignmentResult>> failures = schedulingResult.getFailures();
//                if (!failures.isEmpty()) {
//                    for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry: failures.entrySet()) {
//                        System.out.println("******                failures for task " + entry.getKey().getId());
//                    }
//                }
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, 50L, resultCallback);
        ref.set(schedulingService);
        // let us use VMs having 8 cores and 8000 MB of memory each and.
        // create 2 VMs and fill their usage with A filling 2 CPUs at a time with little memory and B filling 2000 MB
        // at a time with very little CPUs.
        for (int i=0; i<4; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(3, 1000, 1)));
        for (int i=0; i<4; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(1, 3000, 1)));
        schedulingService.start();
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, 8, 8000, 1, 100);
        schedulingService.addLeases(leases);
        if (!unqueueTaskResults(8, assignmentResults))
            Assert.fail("Timeout waiting for 16 tasks");
        // now A is using 12 of the 16 total CPUs in use, and B is using 12,000 of 16,000 total MB, so their
        // dominant resource usage share is equivalent.
        // now submit a task from A to use 1 CPU and 10 MB memory and another task from B to use 1 CPU and 4000 MB memory
        // ensure that they are assigned on a new VM
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(1, 7000, 1)));
        leases = LeaseProvider.getLeases(2, 1, 8, 8000, 1, 100);
        schedulingService.addLeases(leases);
        if (!unqueueTaskResults(2, assignmentResults))
            Assert.fail("Timeout waiting for 2 task assignments");
        // now submit a task from just A with 1 CPU, 10 memory, it should get assigned right away
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 10, 1)));
        if (!unqueueTaskResults(1, assignmentResults))
            Assert.fail("Timeout waiting for 1 task assignment");
        // we now have 3 CPUs and 7020 MB memory being used out of 8 and 8000 respectively
        // now submit 5 tasks from A with 1 CPU, 1 memory each to possibly fill the host as well as a task from B with 1 CPU, 1000 memory.
        // ensure that only the tasks from A get assigned and that the task from B stays queued
        for (int i=0; i<5; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 1, 1)));
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktB, TaskRequestProvider.getTaskRequest(1, 1000, 1)));
        int numTasks = 2;
        while (numTasks > 0) {
            final QueuableTask task = assignmentResults.poll(2000, TimeUnit.MILLISECONDS);
            if (task == null)
                Assert.fail("Timeout waiting for task assignment");
            Assert.assertEquals(tier1bktA.getBucketName(), task.getQAttributes().getBucketName());
            numTasks--;
        }
        final AtomicReference<String> bucketRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        schedulingService.requestAllTasks(new Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>>() {
            @Override
            public void call(Map<TaskQueue.TaskState, Collection<QueuableTask>> stateCollectionMap) {
                final Collection<QueuableTask> tasks = stateCollectionMap.get(TaskQueue.TaskState.QUEUED);
                if (tasks != null && !tasks.isEmpty()) {
                    for (QueuableTask t : tasks)
                        bucketRef.set(t.getQAttributes().getBucketName());
                    latch.countDown();
                }
            }
        });
        if (!latch.await(2000, TimeUnit.MILLISECONDS))
            Assert.fail("Can't get confirmation on task from B to be queued");
        Assert.assertNotNull(bucketRef.get());
        Assert.assertEquals(tier1bktB.getBucketName(), bucketRef.get());
    }

    @Test
    public void testRemoveFromQueue() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                //System.out.println("Got scheduling result with " + schedulingResult.getResultMap().size() + " results");
                if (schedulingResult.getResultMap().size() > 0) {
                    //System.out.println("Assignment on host " + schedulingResult.getResultMap().values().iterator().next().getHostname());
                    latch.countDown();
                }
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, 100L, 200L, resultCallback);
        schedulingService.start();
        final List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 4000, 1, 10);
        schedulingService.addLeases(leases);
        final QueuableTask task = QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(2, 2000, 1));
        queue.queueTask(task);
        if (!latch.await(5, TimeUnit.SECONDS))
            Assert.fail("Did not assign resources in time");
        final CountDownLatch latch2 = new CountDownLatch(1);
        final AtomicBoolean found = new AtomicBoolean();
        schedulingService.requestVmCurrentStates(new Action1<List<VirtualMachineCurrentState>>() {
            @Override
            public void call(List<VirtualMachineCurrentState> states) {
                for (VirtualMachineCurrentState s: states) {
                    for (TaskRequest t: s.getRunningTasks()) {
                        if (t.getId().equals(task.getId())) {
                            found.set(true);
                            latch2.countDown();
                        }
                    }
                }
            }
        });
        if (!latch2.await(5, TimeUnit.SECONDS)) {
            Assert.fail("Didn't get vm states in time");
        }
        Assert.assertTrue("Did not find task on vm", found.get());
        schedulingService.removeTask(task.getId(), task.getQAttributes(), leases.get(0).hostname());
        found.set(false);
        final CountDownLatch latch3 = new CountDownLatch(1);
        schedulingService.requestVmCurrentStates(new Action1<List<VirtualMachineCurrentState>>() {
            @Override
            public void call(List<VirtualMachineCurrentState> states) {
                for (VirtualMachineCurrentState s: states) {
                    for (TaskRequest t: s.getRunningTasks()) {
                        if (t.getId().equals(task.getId())) {
                            found.set(true);
                            latch3.countDown();
                        }
                    }
                }
                latch3.countDown();
            }
        });
        if (!latch3.await(5, TimeUnit.SECONDS)) {
            Assert.fail("Timeout waiting for vm states");
        }
        Assert.assertFalse("Unexpected to find removed task on vm", found.get());
        scheduler.shutdown();
    }

    @Test
    public void testMaxSchedIterDelay() throws Exception {
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                // no-op
            }
        };
        final long maxDelay = 500L;
        final long loopMillis = 50L;
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, loopMillis, maxDelay, resultCallback);
        schedulingService.start();
        long startAt = System.currentTimeMillis();
        Thread.sleep(51L);
        final AtomicLong gotTasksAt = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);
        setupTaskGetter(schedulingService, gotTasksAt, latch);
        if (!latch.await(maxDelay + 100L, TimeUnit.MILLISECONDS)) {
            Assert.fail("Timeout waiting for tasks list");
        }
        Assert.assertTrue("Got task list too soon", (gotTasksAt.get() - startAt) > maxDelay);
        // now test that when queue does change, we get it sooner
        startAt = System.currentTimeMillis();
        latch = new CountDownLatch(1);
        setupTaskGetter(schedulingService, gotTasksAt, latch);
        queue.queueTask(QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1)));
        if (!latch.await(maxDelay + 100L, TimeUnit.MILLISECONDS)) {
            Assert.fail("Timeout waiting for tasks list");
        }
        Assert.assertTrue("Got task list too late", (gotTasksAt.get() - startAt) < (maxDelay + 2 * loopMillis));
        // repeat with adding lease
        startAt = System.currentTimeMillis();
        latch = new CountDownLatch(1);
        setupTaskGetter(schedulingService, gotTasksAt, latch);
        schedulingService.addLeases(LeaseProvider.getLeases(1, 1, 100, 1, 10));
        if (!latch.await(maxDelay + 100L, TimeUnit.MILLISECONDS)) {
            Assert.fail("Timeout waiting for tasks list");
        }
        Assert.assertTrue("Got tasks list too late", (gotTasksAt.get() - startAt) < (maxDelay + 2 * loopMillis));
    }

    @Test
    public void testInitWithPrevRunningTasks() throws Exception {
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                // no-op
            }
        };
        final long maxDelay = 500L;
        final long loopMillis = 50L;
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, loopMillis, maxDelay, resultCallback);
        schedulingService.start();
        final String hostname = "hostA";
        schedulingService.initializeRunningTask(
                QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1)),
                hostname
        );
        final AtomicReference<String> ref = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        schedulingService.requestVmCurrentStates(
                new Action1<List<VirtualMachineCurrentState>>() {
                    @Override
                    public void call(List<VirtualMachineCurrentState> states) {
                        if (states != null && !states.isEmpty()) {
                            final VirtualMachineCurrentState state = states.iterator().next();
                            ref.set(state.getHostname());
                        }
                        latch.countDown();
                    }
                }
        );
        if (!latch.await(maxDelay * 2, TimeUnit.MILLISECONDS)) {
            Assert.fail("Timeout waiting for vm states");
        }
        Assert.assertEquals(hostname, ref.get());
    }

    // Test with a large number of tasks captured from a run that caused problems to tier buckets' sorting. Ensure that
    //
    @Test
    public void testLargeTasksToInitInRunningState() throws Exception {
        final List<SampleLargeNumTasksToInit.Task> runningTasks = SampleLargeNumTasksToInit.getSampleTasksInRunningState();
        System.out.println("GOT " + runningTasks.size() + " tasks");
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        final CountDownLatch latch = new CountDownLatch(6);
        final AtomicReference<List<Exception>> ref = new AtomicReference<>();
        final AtomicBoolean printFailures = new AtomicBoolean();
        Action1<SchedulingResult> resultCallback = new Action1<SchedulingResult>() {
            @Override
            public void call(SchedulingResult schedulingResult) {
                final List<Exception> exceptions = schedulingResult.getExceptions();
                if (exceptions != null && !exceptions.isEmpty())
                    ref.set(exceptions);
                else if (!schedulingResult.getResultMap().isEmpty())
                    System.out.println("#Assignments: " + schedulingResult.getResultMap().values().iterator().next().getTasksAssigned().size());
                else if(printFailures.get()) {
                    final Map<TaskRequest, List<TaskAssignmentResult>> failures = schedulingResult.getFailures();
                    if (!failures.isEmpty()) {
                        for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry: failures.entrySet()) {
                            System.out.println("       Failure for " + entry.getKey().getId() + ":");
                            for(TaskAssignmentResult r: entry.getValue())
                                System.out.println("            " + r.toString());
                        }
                    }
                }
                latch.countDown();
            }
        };
        final long maxDelay = 100L;
        final long loopMillis = 20L;
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, loopMillis, maxDelay, resultCallback);
        Map<String, SampleLargeNumTasksToInit.Task> uniqueTasks = new HashMap<>();
        for(SampleLargeNumTasksToInit.Task t: runningTasks) {
            if (!uniqueTasks.containsKey(t.getBucket()))
                uniqueTasks.put(t.getBucket(), t);
            schedulingService.initializeRunningTask(SampleLargeNumTasksToInit.toQueuableTask(t), t.getHost());
        }
        schedulingService.start();
        // add a few new tasks
        int id=0;
        for(SampleLargeNumTasksToInit.Task t: uniqueTasks.values()) {
            queue.queueTask(
                    SampleLargeNumTasksToInit.toQueuableTask(
                            new SampleLargeNumTasksToInit.Task("newTask-" + id++, t.getBucket(), t.getTier(), t.getCpu(), t.getMemory(), t.getNetworkMbps(), t.getDisk(), null)
                    )
            );
        }
        schedulingService.addLeases(LeaseProvider.getLeases(1000, 1, 32, 500000, 2000, 0, 100));
        Thread.sleep(loopMillis*2);
        printFailures.set(true);
        if (!latch.await(1000, TimeUnit.MILLISECONDS)) {
            Assert.fail("Unexpected to not get enough sched iterations done");
        }

        final List<Exception> exceptions = ref.get();
        if (exceptions != null) {
            for(Exception e: exceptions) {
                if (e instanceof TaskQueueMultiException) {
                    for (Exception ee : ((TaskQueueMultiException) e).getExceptions())
                        ee.printStackTrace();
                }
                else
                    e.printStackTrace();
            }
        }
        Assert.assertNull(exceptions);
    }

    @Test
    public void testNotReadyTask() throws Exception {
        TaskQueue queue = TaskQueues.createTieredQueue(2);
        final TaskScheduler scheduler = getScheduler();
        final AtomicReference<List<Exception>> ref = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<List<String>> assignedTaskIds = new AtomicReference<>(new LinkedList<>());
        final AtomicReference<List<String>> failedTaskIds = new AtomicReference<>(new LinkedList<>());
        Action1<SchedulingResult> resultCallback = schedulingResult -> {
            final List<Exception> exceptions = schedulingResult.getExceptions();
            final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            if (exceptions != null && !exceptions.isEmpty())
                ref.set(exceptions);
            else if (!resultMap.isEmpty()) {
                resultMap.forEach((key, value) -> value.getTasksAssigned().forEach(t -> assignedTaskIds.get().add(t.getTaskId())));
            }
            schedulingResult.getFailures().forEach((t, r) -> failedTaskIds.get().add(t.getId()));
            latch.countDown();
        };
        final long maxDelay = 100L;
        final long loopMillis = 20L;
        final TaskSchedulingService schedulingService = getSchedulingService(queue, scheduler, loopMillis, maxDelay, resultCallback);
        final QueuableTask task1 = QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1));
        queue.queueTask(task1);
        final QueuableTask task2 = QueuableTaskProvider.wrapTask(tier1bktA, TaskRequestProvider.getTaskRequest(1, 100, 1));
        task2.safeSetReadyAt(System.currentTimeMillis() + 1000000L);
        queue.queueTask(task2);
        schedulingService.addLeases(LeaseProvider.getLeases(2, 4, 8000, 2000, 1, 100));
        schedulingService.start();
        if (!latch.await(2, TimeUnit.SECONDS)) {
            Assert.fail("Unexpected to not get assignments in time");
        }
        Assert.assertEquals(1, assignedTaskIds.get().size());
        Assert.assertEquals(task1.getId(), assignedTaskIds.get().iterator().next());
        Assert.assertEquals(0, failedTaskIds.get().size());
        schedulingService.shutdown();
    }

    private void setupTaskGetter(TaskSchedulingService schedulingService, final AtomicLong gotTasksAt, final CountDownLatch latch) throws TaskQueueException {
        schedulingService.requestAllTasks(new Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>>() {
            @Override
            public void call(Map<TaskQueue.TaskState, Collection<QueuableTask>> stateCollectionMap) {
                gotTasksAt.set(System.currentTimeMillis());
                latch.countDown();
            }
        });
    }

    private boolean unqueueTaskResults(int numTasks, BlockingQueue<QueuableTask> assignmentResults) throws InterruptedException {
        while (numTasks > 0) {
            final QueuableTask task = assignmentResults.poll(2000, TimeUnit.MILLISECONDS);
            if (task == null)
                return false;
            else
                numTasks--;
        }
        return true;
    }
}