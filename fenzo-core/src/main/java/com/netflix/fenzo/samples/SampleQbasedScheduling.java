package com.netflix.fenzo.samples;

import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action0;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.queues.TaskQueues;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleQbasedScheduling {

    private static class MesosScheduler implements Scheduler {

        private final AtomicInteger numTasksCompleted;
        private final TaskQueue queue;
        private Action1<List<Protos.Offer>> leaseAction = null;

        MesosScheduler(AtomicInteger numTasksCompleted, TaskQueue queue) {
            this.numTasksCompleted = numTasksCompleted;
            this.queue = queue;
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
                    System.out.println("Task status for " + status.getTaskId() + ": " + status.getState());
                    queue.remove(status.getTaskId().getValue(), qAttribs);
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

    public static void main(String[] args) throws InterruptedException {
        if(args.length!=1) {
            System.err.println("Must provide one argument - Mesos master location string");
            System.exit(1);
        }
        int numTasks=10;
        final AtomicInteger numTasksLaunched = new AtomicInteger();
        final AtomicInteger numTasksCompleted = new AtomicInteger();

        Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
                .setName("Sample Fenzo Framework")
                .setUser("")
                .build();
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
        final TaskQueue queue = TaskQueues.createTieredQueue(2);
        final MesosScheduler mesosScheduler = new MesosScheduler(numTasksCompleted, queue);
        final MesosSchedulerDriver driver = new MesosSchedulerDriver(mesosScheduler, framework, args[0]);
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
                .withTaskQuue(queue)
                .withTaskScheduler(taskScheduler)
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
                                for (TaskAssignmentResult r: e.getValue().getTasksAssigned())
                                    taskInfos.add(SampleFramework.getTaskInfo(
                                            e.getValue().getLeasesUsed().iterator().next().getOffer().getSlaveId(),
                                            r.getTaskId(),
                                            "sleep 10"
                                    ));
                                driver.launchTasks(
                                        offers,
                                        taskInfos
                                );
                            }
                        }
                    }
                })
                .build();
        mesosScheduler.leaseAction = new Action1<List<Protos.Offer>>() {
            @Override
            public void call(List<Protos.Offer> offers) {
                List<VirtualMachineLease> leases = new ArrayList<>();
                for (Protos.Offer o: offers)
                    leases.add(new VMLeaseObject(o));
                schedulingService.addLeases(leases);
            }
        };
        schedulingService.start();
        for (int i=0; i<numTasks; i++)
            queue.queueTask(getTask(i));
        while (numTasksCompleted.get() < numTasks) {
            Thread.sleep(1000);
            System.out.println("        #tasks completed: " + numTasksCompleted.get() + " of " + numTasks);
        }
        System.out.println("ALL DONE");
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
