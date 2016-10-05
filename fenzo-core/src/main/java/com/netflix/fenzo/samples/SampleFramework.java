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

package com.netflix.fenzo.samples;

import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A sample Mesos framework that shows a sample usage of Fenzo {@link TaskScheduler}.
 */
public class SampleFramework {

    /**
     * A sample mesos scheduler that shows how mesos callbacks can be setup for use with Fenzo TaskScheduler.
     */
    public class MesosScheduler implements Scheduler {

        /**
         * When we register successfully with mesos, any previous resource offers are invalid. Tell Fenzo scheduler
         * to expire all leases (aka offers) right away.
         */
        @Override
        public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
            System.out.println("Registered! ID = " + frameworkId.getValue());
            scheduler.expireAllLeases();
        }

        /**
         * Similar to {@code registered()} method, expire any previously known resource offers by asking Fenzo
         * scheduler to expire all leases right away.
         */
        @Override
        public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
            System.out.println("Re-registered " + masterInfo.getId());
            scheduler.expireAllLeases();
        }

        /**
         * Add the received Mesos resource offers to the lease queue. Fenzo scheduler is used by calling its main
         * allocation routine in a loop, see {@link SampleFramework#runAll()}. Collect offers from mesos into a queue
         * so the next call to Fenzo's allocation routine can pick them up.
         */
        @Override
        public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
            for(Protos.Offer offer: offers) {
                System.out.println("Adding offer " + offer.getId() + " from host " + offer.getHostname());
                leasesQueue.offer(new VMLeaseObject(offer));
            }
        }

        /**
         * Tell Fenzo scheduler that a resource offer should be expired immediately.
         */
        @Override
        public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
            scheduler.expireLease(offerId.getValue());
        }

        /**
         * Update Fenzo scheduler of task completion if received status indicates a terminal state. There is no need
         * to tell Fenzo scheduler of task started because that is supposed to have been already done before launching
         * the task in Mesos.
         *
         * In a real world framework, this state change would also be persisted with a state machine of choice.
         */
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

        /**
         * Upon slave lost notification, tell Fenzo scheduler to expire all leases with the given slave ID. Note, however,
         * that if there was no offer received from that slave prior to this call, Fenzo would not have a mapping from
         * the slave ID to hostname (Fenzo maintains slaves state by hostname). This is OK since there would be no offers
         * to expire. However, any tasks running on the lost slave will not be removed by this call to Fenzo. Task lost
         * status updates would ensure that.
         */
        @Override
        public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
            scheduler.expireAllLeasesByVMId(slaveId.getValue());
        }

        /**
         * Do nothing, instead, rely on task lost status updates to inform Fenzo of task completions.
         */
        @Override
        public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
            System.out.println("Executor " + executorId.getValue() + " lost, status=" + status);
        }

        @Override
        public void error(SchedulerDriver driver, String message) {}
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

    /**
     * Create a sample mesos framework with the given task queue and mesos master connection string. As would be typical
     * for frameworks that wish to use Fenzo task scheduler, a lease queue is created for mesos scheduler callback to
     * insert offers received from mesos. This sample implementation obtains the tasks to run via a task queue. The
     * {@link SampleFramework#runAll()} method implements the scheduling loop that continuously takes pending tasks from
     * the queue and uses Fenzo's task scheduler to assign resources to them.
     *
     * The task scheduler created in this sample is a rather simple one, with no advanced features.
     *
     * @param taskQueue The task queue.
     * @param mesosMaster Connection string for mesos master.
     * @param onTaskComplete A single argument action trigger to invoke upon task completion, with task ID is the argument.
     * @param taskCmdGetter A single argument function to invoke to get the command line to execute for a given task ID,
     *                      passed as the only argument.
     */
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

    /**
     * Shuts down the Mesos driver.
     */
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

    /**
     * Run scheduling loop until shutdown is called.
     * This sample implementation shows the general pattern of using Fenzo's task scheduler. Scheduling occurs in a
     * continuous loop, iteratively calling {@link TaskScheduler#scheduleOnce(List, List)} method, passing in the
     * list of tasks currently pending launch, and a list of any new resource offers obtained from mesos since the last
     * time the lease queue was drained. The call returns an assignments result object from which any tasks assigned
     * can be launched via the mesos driver. The result contains a map with hostname as the key and assignment result
     * as the value. The assignment result contains the resource offers of the host that were used for the assignments
     * and a list of tasks assigned resources from the host. The resource offers were removed from the internal state in
     * Fenzo. However, the task assignments are not updated, yet. If the returned assignments are being used to launch
     * the tasks in mesos, call Fenzo's task assigner for each task launched to indicate that the task is being launched.
     * If the returned assignments are not being used, the resource offers in the assignment results must either be
     * rejected in mesos, or added back into Fenzo explicitly.
     */
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
                    final Protos.SlaveID slaveId = leasesUsed.get(0).getOffer().getSlaveId();
                    for(TaskAssignmentResult t: result.getTasksAssigned()) {
                        stringBuilder.append(t.getTaskId()).append(", ");
                        taskInfos.add(getTaskInfo(slaveId, t.getTaskId(), taskCmdGetter.call(t.getTaskId())));
                        // unqueueTask task from pending tasks map and put into launched tasks map
                        // (in real world, transition the task state)
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
            // insert a short delay before scheduling any new tasks or tasks from before that haven't been launched yet.
            try{Thread.sleep(100);}catch(InterruptedException ie){}
        }
    }

    static Protos.TaskInfo getTaskInfo(Protos.SlaveID slaveID, final String taskId, String cmd) {
        Protos.TaskID pTaskId = Protos.TaskID.newBuilder()
                .setValue(taskId).build();
        return Protos.TaskInfo.newBuilder()
                .setName("task " + pTaskId.getValue())
                .setTaskId(pTaskId)
                .setSlaveId(slaveID)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
                .setCommand(Protos.CommandInfo.newBuilder().setValue(cmd).build())
                .build();
    }

}
