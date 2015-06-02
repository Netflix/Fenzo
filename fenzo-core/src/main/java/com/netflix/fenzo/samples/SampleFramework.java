package com.netflix.fenzo.samples;

import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SampleFramework {

    public class MesosScheduler implements Scheduler {

        @Override
        public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
            System.out.println("Registered! ID = " + frameworkId.getValue());
            scheduler.expireAllLeases();
        }
        @Override
        public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
            System.out.println("Re-registered " + masterInfo.getId());
            scheduler.expireAllLeases();
        }

        @Override
        public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
            for(Protos.Offer offer: offers) {
                System.out.println("Adding offer " + offer.getId() + " from host " + offer.getHostname());
                leasesQueue.offer(new VMLeaseObject(offer));
            }
        }

        @Override
        public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
            scheduler.expireLease(offerId.getValue());
        }

        @Override
        public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
            System.out.println("Task Update: " + status.getTaskId().getValue() + " in state " + status.getState());
            switch (status.getState()) {
                case TASK_FAILED:
                case TASK_LOST:
                case TASK_FINISHED:
                    onTaskComplete.call(status.getTaskId().getValue());
                    scheduler.getTaskUnAssigner().call(status.getTaskId().getValue(), launchedTasks.get(status.getTaskId().getValue()));
                    break;
            }
        }

        @Override
        public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {}

        @Override
        public void disconnected(SchedulerDriver driver) {}

        @Override
        public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {}

        @Override
        public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
            System.out.println("Executor " + executorId.getValue() + " lost, status=" + status);
        }

        @Override
        public void error(SchedulerDriver driver, String message) {}
    }

    private static class VMLeaseObject implements VirtualMachineLease {
        private Protos.Offer offer;
        private double cpuCores;
        private double memoryMB;
        private double networkMbps=0.0;
        private double diskMB;
        private String hostname;
        private String vmID;
        private List<Range> portRanges;
        private Map<String, Protos.Attribute> attributeMap;
        private long offeredTime;
        VMLeaseObject(Protos.Offer offer) {
            this.offer = offer;
            hostname = offer.getHostname();
            this.vmID = offer.getSlaveId().getValue();
            offeredTime = System.currentTimeMillis();
            // parse out resources from offer
            // expects network bandwidth to come in as consumable scalar resource named "network"
            for (Protos.Resource resource : offer.getResourcesList()) {
                if ("cpus".equals(resource.getName())) {
                    cpuCores = resource.getScalar().getValue();
                } else if ("mem".equals(resource.getName())) {
                    memoryMB = resource.getScalar().getValue();
                } else if("network".equals(resource.getName())) {
                    networkMbps = resource.getScalar().getValue();
                } else if ("disk".equals(resource.getName())) {
                    diskMB = resource.getScalar().getValue();
                } else if ("ports".equals(resource.getName())) {
                    portRanges = new ArrayList<>();
                    for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
                        portRanges.add(new Range((int)range.getBegin(), (int) range.getEnd()));
                    }
                }
            }
            attributeMap = new HashMap<>();
            if(offer.getAttributesCount()>0) {
                for(Protos.Attribute attribute: offer.getAttributesList()) {
                    attributeMap.put(attribute.getName(), attribute);
                }
            }
        }
        @Override
        public String hostname() {
            return hostname;
        }
        @Override
        public String getVMID() {
            return vmID;
        }
        @Override
        public double cpuCores() {
            return cpuCores;
        }
        @Override
        public double memoryMB() {
            return memoryMB;
        }
        @Override
        public double networkMbps() {
            return networkMbps;
        }
        @Override
        public double diskMB() {
            return diskMB;
        }
        public Protos.Offer getOffer(){
            return offer;
        }
        @Override
        public String getId() {
            return offer.getId().getValue();
        }
        @Override
        public long getOfferedTime() {
            return offeredTime;
        }
        @Override
        public List<Range> portRanges() {
            return portRanges;
        }
        @Override
        public Map<String, Protos.Attribute> getAttributeMap() {
            return attributeMap;
        }
    }

    private final BlockingQueue<TaskRequest> taskQueue;
    private final BlockingQueue<VirtualMachineLease> leasesQueue;
    private final Map<String, String> launchedTasks;
    private final TaskScheduler scheduler;
    private final MesosSchedulerDriver mesosSchedulerDriver;
    private final AtomicReference<MesosSchedulerDriver> ref = new AtomicReference<>();
    private final Map<String, TaskRequest> pendingTasksMap = new HashMap<>();
    private final Action1<String> onTaskComplete;
    private final Func1<String, String> taskCmdGetter;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public SampleFramework(BlockingQueue<TaskRequest> taskQueue, String mesosMaster, Action1<String> onTaskComplete,
                           Func1<String, String> taskCmdGetter) {
        this.taskQueue = taskQueue;
        this.leasesQueue = new LinkedBlockingQueue<>();
        this.onTaskComplete = onTaskComplete;
        this.taskCmdGetter = taskCmdGetter;
        launchedTasks = new HashMap<>();
        scheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        System.out.println("Declining offer on " + lease.hostname());
                        ref.get().declineOffer(lease.getOffer().getId());
                    }
                })
                .build();
        Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
                .setName("Sample Fenzo Framework")
                .setUser("")
                .build();
        Scheduler mesosScheduler = new MesosScheduler();
        mesosSchedulerDriver = new MesosSchedulerDriver(mesosScheduler, framework, mesosMaster);
        ref.set(mesosSchedulerDriver);
        new Thread() {
            public void run() {
                mesosSchedulerDriver.run();
            }
        }.start();
    }

    public void shutdown() {
        System.out.println("Stopping down mesos driver");
        Protos.Status status = mesosSchedulerDriver.stop();
        isShutdown.set(true);
    }

    public void start() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                runAll();
            }
        }).start();
    }

    void runAll() {
        System.out.println("Running all");
        List<VirtualMachineLease> newLeases = new ArrayList<>();
        while(true) {
            if(isShutdown.get())
                return;
            newLeases.clear();
            List<TaskRequest> newTaskRequests = new ArrayList<>();
            System.out.println("#Pending tasks: " + pendingTasksMap.size());
            TaskRequest taskRequest=null;
            try {
                taskRequest = pendingTasksMap.size()==0?
                        taskQueue.poll(5, TimeUnit.SECONDS) :
                        taskQueue.poll(1, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ie) {
                System.err.println("Error polling task queue: " + ie.getMessage());
            }
            if(taskRequest!=null) {
                taskQueue.drainTo(newTaskRequests);
                newTaskRequests.add(0, taskRequest);
                for(TaskRequest request: newTaskRequests)
                    pendingTasksMap.put(request.getId(), request);
            }
            leasesQueue.drainTo(newLeases);
            SchedulingResult schedulingResult = scheduler.scheduleOnce(new ArrayList<>(pendingTasksMap.values()), newLeases);
            System.out.println("result=" + schedulingResult);
            Map<String,VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            if(!resultMap.isEmpty()) {
                for(VMAssignmentResult result: resultMap.values()) {
                    List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
                    List<Protos.TaskInfo> taskInfos = new ArrayList<>();
                    StringBuilder stringBuilder = new StringBuilder("Launching on VM " + leasesUsed.get(0).hostname() + " tasks ");
                    for(TaskAssignmentResult t: result.getTasksAssigned()) {
                        stringBuilder.append(t.getTaskId()).append(", ");
                        taskInfos.add(getTaskInfo(leasesUsed, t.getTaskId()));
                        pendingTasksMap.remove(t.getTaskId());
                        launchedTasks.put(t.getTaskId(), leasesUsed.get(0).hostname());
                        scheduler.getTaskAssigner().call(t.getRequest(), leasesUsed.get(0).hostname());
                    }
                    List<Protos.OfferID> offerIDs = new ArrayList<>();
                    for(VirtualMachineLease l: leasesUsed)
                        offerIDs.add(l.getOffer().getId());
                    System.out.println(stringBuilder.toString());
                    mesosSchedulerDriver.launchTasks(offerIDs, taskInfos);
                }
            }
            // sleep a bit before looping for new tasks to launch
            try{Thread.sleep(100);}catch(InterruptedException ie){}
        }
    }
    private Protos.TaskInfo getTaskInfo(List<VirtualMachineLease> leasesUsed, final String taskId) {
        Protos.TaskID pTaskId = Protos.TaskID.newBuilder()
                .setValue(taskId).build();
        return Protos.TaskInfo.newBuilder()
                .setName("task " + pTaskId.getValue())
                .setTaskId(pTaskId)
                .setSlaveId(leasesUsed.get(0).getOffer().getSlaveId())
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
                .setCommand(Protos.CommandInfo.newBuilder().setValue(taskCmdGetter.call(taskId)).build())
                .build();
    }

}
