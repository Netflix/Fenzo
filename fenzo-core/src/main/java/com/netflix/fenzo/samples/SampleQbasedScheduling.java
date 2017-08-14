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

package com.netflix.fenzo.samples;

import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action0;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.fenzo.queues.*;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SampleQbasedScheduling {

    private static class MesosScheduler implements Scheduler {

        private final AtomicInteger numTasksCompleted;
        private final AtomicReference<TaskSchedulingService> schedSvcGetter;
        private Action1<List<Protos.Offer>> leaseAction = null;

        MesosScheduler(AtomicInteger numTasksCompleted, AtomicReference<TaskSchedulingService> schedSvcGetter) {
            this.numTasksCompleted = numTasksCompleted;
            this.schedSvcGetter = schedSvcGetter;
        }

        @Override
        public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
            System.out.println("Mesos scheduler registered");
        }

        @Override
        public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
            System.out.println("Mesos scheduler re-registered");
        }

        @Override
        public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
            leaseAction.call(offers);
        }

        @Override
        public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
            System.out.println("Unexpected offers Rescinded");
        }

        @Override
        public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
            switch (status.getState()) {
                case TASK_FAILED:
                case TASK_LOST:
                case TASK_FINISHED:
                    System.out.println("Task status for " + status.getTaskId().getValue() + ": " + status.getState());
                    schedSvcGetter.get().removeTask(status.getTaskId().getValue(),
                            allTasks.get(status.getTaskId().getValue()).getQAttributes(),
                            tasksToHostnameMap.get(status.getTaskId().getValue()));
                    numTasksCompleted.incrementAndGet();
            }
        }

        @Override
        public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {

        }

        @Override
        public void disconnected(SchedulerDriver driver) {
            System.out.println("Mesos driver disconnected");
        }

        @Override
        public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
            System.out.println("Mesos agent lost");
        }

        @Override
        public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
            System.out.println("Mesos executor lost");
        }

        @Override
        public void error(SchedulerDriver driver, String message) {
            System.out.println("Unexpected mesos scheduler error: " + message);
        }
    }

    private final static QAttributes qAttribs = new QAttributes.QAttributesAdaptor(0, "onlyBucket");

    private final static ConcurrentMap<String, QueuableTask> allTasks = new ConcurrentHashMap<>();
    private final static ConcurrentMap<String, String> tasksToHostnameMap = new ConcurrentHashMap<>();

    /**
     * This is the main method of this sample framework. It showcases how to use Fenzo queues for scheduling. It creates
     * some number of tasks and launches them into Mesos using the Mesos built-in command executor. The tasks launched
     * are simple sleep commands that sleep for 3 seconds each.
     * @param args Requires one argument for the location of Mesos master to connect to.
     * @throws Exception Upon catching any exceptions within the program.
     */
    public static void main(String[] args) throws Exception {
        if(args.length!=1) {
            System.err.println("Must provide one argument - Mesos master location string");
            System.exit(1);
        }
        int numTasks=10;
        final AtomicInteger numTasksCompleted = new AtomicInteger();

        // create Fenzo TaskScheduler object.
        final TaskScheduler taskScheduler = new TaskScheduler.Builder()
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker)
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease v) {
                        System.out.println("Unexpected to reject lease on " + v.hostname());
                    }
                })
                .build();

        // Create a queue from Fenzo provided queue implementations.
        final TaskQueue queue = TaskQueues.createTieredQueue(2);

        // Create our Mesos scheduler callback implementation
        AtomicReference<TaskSchedulingService> schedSvcGetter = new AtomicReference<>();
        final MesosScheduler mesosSchedulerCallback = new MesosScheduler(numTasksCompleted, schedSvcGetter);

        // create Mesos driver
        Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
                .setName("Sample Fenzo Framework")
                .setUser("")
                .build();
        final MesosSchedulerDriver driver = new MesosSchedulerDriver(mesosSchedulerCallback, framework, args[0]);

        // Build Fenzo task scheduling service using the TaskScheduler and TaskQueue objects created above
        final AtomicInteger schedCounter = new AtomicInteger();
        final TaskSchedulingService schedulingService = new TaskSchedulingService.Builder()
                .withLoopIntervalMillis(1000)
                .withMaxDelayMillis(1500)
                .withPreSchedulingLoopHook(new Action0() {
                    @Override
                    public void call() {
                        System.out.println("Starting scheduling iteration " + schedCounter.incrementAndGet());
                    }
                })
                .withTaskQueue(queue)
                .withTaskScheduler(taskScheduler)
                // TaskSchedulingService will call us back when there are task assignments. Handle them by launching
                // tasks using MesosDriver
                .withSchedulingResultCallback(new Action1<SchedulingResult>() {
                    @Override
                    public void call(SchedulingResult schedulingResult) {
                        final List<Exception> exceptions = schedulingResult.getExceptions();
                        if (exceptions != null && !exceptions.isEmpty()) {
                            System.out.println("Exceptions from scheduling iteration:");
                            for (Exception e: exceptions)
                                e.printStackTrace();
                        }
                        else {
                            for (Map.Entry<String, VMAssignmentResult> e: schedulingResult.getResultMap().entrySet()) {
                                List<Protos.OfferID> offers = new ArrayList<Protos.OfferID>();
                                for (VirtualMachineLease l: e.getValue().getLeasesUsed())
                                    offers.add(l.getOffer().getId());
                                List<Protos.TaskInfo> taskInfos = new ArrayList<Protos.TaskInfo>();
                                for (TaskAssignmentResult r: e.getValue().getTasksAssigned()) {
                                    taskInfos.add(SampleFramework.getTaskInfo(
                                            e.getValue().getLeasesUsed().iterator().next().getOffer().getSlaveId(),
                                            r.getTaskId(),
                                            "sleep 2"
                                    ));
                                    tasksToHostnameMap.put(r.getTaskId(), r.getHostname());
                                }
                                driver.launchTasks(
                                        offers,
                                        taskInfos
                                );
                            }
                        }
                    }
                })
                .build();
        schedSvcGetter.set(schedulingService);

        // set up action in our scheduler callback to send resource offers into our scheduling service
        mesosSchedulerCallback.leaseAction = new Action1<List<Protos.Offer>>() {
            @Override
            public void call(List<Protos.Offer> offers) {
                List<VirtualMachineLease> leases = new ArrayList<>();
                for (Protos.Offer o: offers)
                    leases.add(new VMLeaseObject(o)); // Fenzo lease object adapter for Mesos Offer object
                schedulingService.addLeases(leases);
            }
        };
        schedulingService.start();

        // start Mesos driver
        new Thread() {
            @Override
            public void run() {
                driver.run();
            }
        }.start();

        // submit some tasks
        for (int i=0; i<numTasks; i++) {
            final QueuableTask task = getTask(i);
            allTasks.put(task.getId(), task);
            queue.queueTask(task);
        }

        // wait for tasks to complete
        while (numTasksCompleted.get() < numTasks) {
            Thread.sleep(1000);
            System.out.println("        #tasks completed: " + numTasksCompleted.get() + " of " + numTasks);
        }

        // verify that Fenzo has no tasks in its queue
        final CountDownLatch latch = new CountDownLatch(1);
        schedulingService.requestAllTasks(new Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>>() {
            @Override
            public void call(Map<TaskQueue.TaskState, Collection<QueuableTask>> taskStateCollectionMap) {
                System.out.println("Fenzo queue has " + taskStateCollectionMap.size() + " items");
                latch.countDown();
            }
        });
        if (!latch.await(5, TimeUnit.SECONDS))
            System.err.println("Timeout waiting for listing all tasks in Fenzo queues");

        System.out.println("ALL DONE");
        System.exit(0);
    }

    private static QueuableTask getTask(final int i) {
        return new QueuableTask() {
            @Override
            public QAttributes getQAttributes() {
                return qAttribs;
            }

            @Override
            public String getId() {
                return "Task-" + i;
            }

            @Override
            public String taskGroupName() {
                return "groupA";
            }

            @Override
            public double getCPUs() {
                return 1;
            }

            @Override
            public double getMemory() {
                return 100;
            }

            @Override
            public double getNetworkMbps() {
                return 0;
            }

            @Override
            public double getDisk() {
                return 0;
            }

            @Override
            public int getPorts() {
                return 0;
            }

            @Override
            public Map<String, Double> getScalarRequests() {
                return null;
            }

            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return null;
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }

            @Override
            public void setAssignedResources(AssignedResources assignedResources) {
                // no-op, unexpected for this sample
            }

            @Override
            public AssignedResources getAssignedResources() {
                return null;
            }
        };
    }
}
