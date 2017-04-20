/*
 * Copyright 2017 Netflix, Inc.
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
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.queues.TaskQueues;
import com.netflix.fenzo.queues.tiered.QueuableTaskProvider;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ShortfallAutoscalerTest {

    private static String hostAttrName = "MachineType";
    private final int cpus1=4;
    private final int cpus2=8;
    private final int memMultiplier=1000;
    private final int minIdle1 =1;
    private final int maxIdle1 =1;
    private final int minIdle2 =1;
    private final int maxIdle2 =1;
    private final int maxSize1=4;
    private final int maxSize2=10;
    private final long coolDownSecs=5;
    private final String hostAttrVal1="4coreServers";
    private final String hostAttrVal2="4cS2";
    private final Map<String, Protos.Attribute> attributes1 = new HashMap<>();
    private final Map<String, Protos.Attribute> attributes2 = new HashMap<>();
    private final List<VirtualMachineLease.Range> ports = new ArrayList<>();
    private final QAttributes qA1 = new QAttributes.QAttributesAdaptor(0, "bucketA");
    private final QAttributes qA2 = new QAttributes.QAttributesAdaptor(0, "bucketB");

    @Before
    public void setUp() throws Exception {
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1)).build();
        attributes1.put(hostAttrName, attribute);
        Protos.Attribute attribute2 = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal2)).build();
        attributes2.put(hostAttrName, attribute2);
        ports.add(new VirtualMachineLease.Range(1, 100));
    }

    private TaskScheduler getScheduler(final boolean expectLeaseRejection, final Action1<AutoScaleAction> callback,
                                       long delayScaleUpBySecs, long delayScaleDownByDecs,
                                       AutoScaleRule... rules) {
        return AutoScalerTest.getScheduler(expectLeaseRejection, callback, delayScaleUpBySecs, delayScaleDownByDecs,
                rules);
    }

    private TaskSchedulingService getSchedulingService(TaskQueue queue, Action0 preHook, TaskScheduler scheduler,
                                                       Action1<SchedulingResult> resultCallback) {
        return new TaskSchedulingService.Builder()
                .withLoopIntervalMillis(50)
                .withMaxDelayMillis(500)
                .withPreSchedulingLoopHook(preHook)
                .withSchedulingResultCallback(resultCallback)
                .withTaskQuue(queue)
                .withTaskScheduler(scheduler)
                .withOptimizingShortfallEvaluator()
                .build();
    }

    @Test
    public void testShortfallScaleUp1group() throws Exception {
        final AutoScaleRule rule = AutoScaleRuleProvider.createRule(hostAttrVal1, minIdle1, maxIdle1, coolDownSecs,
                1, 1000);
        BlockingQueue<Integer> scaleUpReqQ = new LinkedBlockingQueue<>();
        Action1<AutoScaleAction> callback = (action) -> {
            if (action instanceof ScaleUpAction) {
                scaleUpReqQ.offer(((ScaleUpAction) action).getScaleUpCount());
            }
        };
        TaskScheduler scheduler = getScheduler(true, callback, 0, 0, rule);
        Action0 preHook = () -> {};
        BlockingQueue<SchedulingResult> resultQ = new LinkedBlockingQueue<>();
        TaskQueue queue = TaskQueues.createTieredQueue(1);
        Action1<SchedulingResult> resultCallback = result -> {
            final List<Exception> exceptions = result.getExceptions();
            if (exceptions != null && !exceptions.isEmpty()) {
                for (Exception e: exceptions)
                    e.printStackTrace();
                Assert.fail("Exceptions in scheduling result");
            } else {
                final Map<TaskRequest, List<TaskAssignmentResult>> failures = result.getFailures();
                if (failures != null && !failures.isEmpty()) {
                    printFailures(failures);
                }
                resultQ.offer(result);
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(
                queue, preHook, scheduler, resultCallback);
        final List<QueuableTask> requests = new ArrayList<>();
        for(int i = 0; i<rule.getMaxIdleHostsToKeep()*8* cpus1; i++)
            requests.add(QueuableTaskProvider.wrapTask(qA1, TaskRequestProvider .getTaskRequest(1, memMultiplier, 1)));
        System.out.println("Created " + requests.size() + " tasks");
        final List<VirtualMachineLease> leases = new ArrayList<>();
        leases.add(LeaseProvider.getLeaseOffer("host1", cpus1, cpus1 * 1000, ports, attributes1));
        leases.add(LeaseProvider.getLeaseOffer("host2", cpus1, cpus1 * 1000, ports, attributes1));
        for (QueuableTask t: requests) {
            queue.queueTask(t);
        }
        schedulingService.addLeases(leases);
        schedulingService.start();
        SchedulingResult result = resultQ.poll(1, TimeUnit.SECONDS);
        Assert.assertNotNull("Timeout waiting for result", result);

        Integer scaleUpNoticed = scaleUpReqQ.poll(coolDownSecs * 2, TimeUnit.SECONDS);
        Assert.assertNotNull(scaleUpNoticed);
        int expected = (requests.size() - (leases.size()* cpus1))/ cpus1;
        Assert.assertEquals(expected, scaleUpNoticed.intValue());
        requests.clear();

        final int newRequests = rule.getMaxIdleHostsToKeep() * 3 * cpus1;
        for(int i=0; i<newRequests; i++)
            queue.queueTask(QueuableTaskProvider.wrapTask(qA1, TaskRequestProvider.getTaskRequest(1, 1000, 1)));
        result = resultQ.poll(1, TimeUnit.SECONDS);
        Assert.assertNotNull(result);
        expected = newRequests;
        expected /= cpus1;
        scaleUpNoticed = scaleUpReqQ.poll(coolDownSecs, TimeUnit.SECONDS);
        Assert.assertNotNull(scaleUpNoticed);
        Assert.assertEquals(expected, scaleUpNoticed.intValue());
        schedulingService.shutdown();
    }

    @Test
    public void testShortfallScaleup2groups() throws Exception {
        final AutoScaleRule rule1 = AutoScaleRuleProvider.createWithMaxSize(hostAttrVal1, minIdle1, maxIdle1, coolDownSecs,
                1, 1000, maxSize1);
        final AutoScaleRule rule2 = AutoScaleRuleProvider.createWithMaxSize(hostAttrVal2, minIdle2, maxIdle2, coolDownSecs,
                1, 1000, maxSize2);
        BlockingQueue<Map<String, Integer>> scaleupActionsQ = new LinkedBlockingQueue<>();
        Action1<AutoScaleAction> callback = (action) -> {
            if (action instanceof ScaleUpAction) {
                scaleupActionsQ.offer(Collections.singletonMap(action.getRuleName(),
                        ((ScaleUpAction) action).getScaleUpCount()));
            }
        };
        TaskScheduler scheduler = getScheduler(true, callback, 0, 0, rule1, rule2);
        Action0 preHook = () -> {};
        BlockingQueue<SchedulingResult> resultQ = new LinkedBlockingQueue<>();
        TaskQueue queue = TaskQueues.createTieredQueue(1);

        // create 3 VMs for group1 and 1 VM for group2
        int hostIdx=0;
        final List<VirtualMachineLease> leases = new ArrayList<>();
        leases.add(LeaseProvider.getLeaseOffer("host" + hostIdx++, cpus1, cpus1 * memMultiplier, ports, attributes1));
        leases.add(LeaseProvider.getLeaseOffer("host" + hostIdx++, cpus1, cpus1 * memMultiplier, ports, attributes1));
        leases.add(LeaseProvider.getLeaseOffer("host" + hostIdx++, cpus1, cpus1 * memMultiplier, ports, attributes1));
        leases.add(LeaseProvider.getLeaseOffer("host" + hostIdx++, cpus2, cpus2 * memMultiplier, ports, attributes2));
        // create 1-cpu tasks to fill VMS equal to two times the max size of group1, and also group2
        for (int i=0; i < (cpus1*rule1.getMaxSize() + cpus2* rule2.getMaxSize()); i++) {
            queue.queueTask(QueuableTaskProvider.wrapTask(qA1, TaskRequestProvider.getTaskRequest(1, memMultiplier, 1)));
            queue.queueTask(QueuableTaskProvider.wrapTask(qA2, TaskRequestProvider.getTaskRequest(1, memMultiplier, 1)));
        }
        // setup scheduling service
        Action1<SchedulingResult> resultCallback = result -> {
            final List<Exception> exceptions = result.getExceptions();
            if (exceptions != null && !exceptions.isEmpty()) {
                for (Exception e: exceptions)
                    e.printStackTrace();
                Assert.fail("Exceptions in scheduling result");
            } else {
                final Map<TaskRequest, List<TaskAssignmentResult>> failures = result.getFailures();
                if (failures != null && !failures.isEmpty()) {
                    printFailures(failures);
                }
                resultQ.offer(result);
            }
        };
        final TaskSchedulingService schedulingService = getSchedulingService(
                queue, preHook, scheduler, resultCallback);
        // wait for scheduling result
        schedulingService.addLeases(leases);
        schedulingService.start();
        final SchedulingResult result = resultQ.poll(1, TimeUnit.SECONDS);
        Assert.assertNotNull(result);
        Assert.assertEquals(3*cpus1 + cpus2, getNumTasksAssigned(result));
        // wait for scale up actions
        int waitingFor = 2;
        while (waitingFor > 0) {
            final Map<String, Integer> map = scaleupActionsQ.poll(coolDownSecs, TimeUnit.SECONDS);
            Assert.assertNotNull(map);
            for (Map.Entry<String, Integer> entry: map.entrySet()) {
                waitingFor--;
                switch (entry.getKey()) {
                    case hostAttrVal1:
                        Assert.assertEquals(rule1.getMaxSize() - 3, entry.getValue().intValue());
                        break;
                    case hostAttrVal2:
                        Assert.assertEquals(rule2.getMaxSize() - 1, entry.getValue().intValue());
                        break;
                    default:
                        Assert.fail("Unknown scale up group: " + entry.getKey() + " for " + entry.getValue() + " VMs");
                }
            }
        }
        schedulingService.shutdown();
    }

    private int getNumTasksAssigned(SchedulingResult result) {
        if (result == null)
            return 0;
        final Map<String, VMAssignmentResult> resultMap = result.getResultMap();
        if (resultMap == null || resultMap.isEmpty())
            return 0;
        int n=0;
        for (VMAssignmentResult r: resultMap.values())
            n += r.getTasksAssigned().size();
        return n;
    }

    private void printFailures(Map<TaskRequest, List<TaskAssignmentResult>> failures) {
//        for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry: failures.entrySet()) {
//            System.out.println("************** Failures for task " + entry.getKey().getId());
//            for (TaskAssignmentResult r: entry.getValue())
//                for (AssignmentFailure f: r.getFailures())
//                    System.out.println("***************        " + r.getHostname() + "::" + f.toString());
//        }
    }
}
