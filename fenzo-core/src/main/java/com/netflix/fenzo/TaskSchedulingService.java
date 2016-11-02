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
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.queues.*;
import com.netflix.fenzo.queues.TaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A task scheduling service that maintains a scheduling loop to continuously assign resources to tasks pending in
 * the queue. This service maintains a scheduling loop to assign resources to tasks in the queue created when
 * constructing this service. It calls {@link TaskScheduler#scheduleOnce(TaskIterator, List)} from within its
 * scheduling loop. Users of this service add tasks into this service's queue, which are held until they are assigned.
 * Here's a typical use of this service:
 * <UL>
 *     <LI>
 *         Build a {@link TaskScheduler} using its builder, {@link TaskScheduler.Builder}.
 *     </LI>
 *     <LI>
 *         Build this service using its builder, {@link TaskSchedulingService.Builder}, providing a queue implementation
 *         from {@link com.netflix.fenzo.queues.TaskQueues}. Specify scheduling interval and other callbacks.
 *     </LI>
 *     <LI>
 *         Start the scheduling loop by calling {@link #start()}.
 *     </LI>
 *     <LI>
 *         Receive callbacks for scheduling result that provide a {@link SchedulingResult} object. Note that it is
 *         not allowed to call {@link TaskScheduler#getTaskAssigner()} for tasks assigned in the result, they are
 *         assigned from within this scheduling service. This service assigns the tasks before making the result
 *         available to you via the callback. To mark tasks as running for those tasks that were running from
 *         before this service was created, use {@link #initializeRunningTask(QueuableTask, String)}. Later, call
 *         {@link #removeTask(String, QAttributes, String)} when tasks complete or they no longer need resource assignments.
 *     </LI>
 * </UL>
 */
public class TaskSchedulingService {

    private static class RemoveTaskRequest {
        private final String taskId;
        private final QAttributes qAttributes;
        private final String hostname;

        public RemoveTaskRequest(String taskId, QAttributes qAttributes, String hostname) {
            this.taskId = taskId;
            this.qAttributes = qAttributes;
            this.hostname = hostname;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulingService.class);
    private final TaskScheduler taskScheduler;
    private final Action1<SchedulingResult> schedulingResultCallback;
    private final InternalTaskQueue taskQueue;
    private final ScheduledExecutorService executorService;
    private final long loopIntervalMillis;
    private final Action0 preHook;
    private final BlockingQueue<VirtualMachineLease> leaseBlockingQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Map<String, QueuableTask>> addRunningTasksQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<RemoveTaskRequest> removeTasksQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>>> taskMapRequest = new LinkedBlockingQueue<>(10);
    private final BlockingQueue<Action1<Map<String, Map<VMResource, Double[]>>>> resStatusRequest = new LinkedBlockingQueue<>(10);
    private final BlockingQueue<Action1<List<VirtualMachineCurrentState>>> vmCurrStateRequest = new LinkedBlockingQueue<>(10);
    private final AtomicLong lastSchedIterationAt = new AtomicLong();
    private final long maxSchedIterDelay;
    private volatile Func1<QueuableTask, List<String>> taskToClusterAutoScalerMapGetter = null;

    private TaskSchedulingService(Builder builder) {
        taskScheduler = builder.taskScheduler;
        schedulingResultCallback = builder.schedulingResultCallback;
        taskQueue = builder.taskQueue;
        taskScheduler.getTaskTracker().setUsageTrackedQueue(taskQueue.getUsageTracker());
        taskScheduler.setUsingSchedulingService(true);
        executorService = builder.executorService;
        loopIntervalMillis = builder.loopIntervalMillis;
        preHook = builder.preHook;
        maxSchedIterDelay = Math.max(builder.maxDelayMillis, loopIntervalMillis);
    }

    /**
     * Start this scheduling service. Tasks are scheduled continuously in a loop with a delay between two consecutive
     * iterations of at least the value specified via {@link Builder#withLoopIntervalMillis(long)}, and at most delay
     * specified via {@link Builder#withMaxDelayMillis(long)}. The delay between consecutive iterations is longer if the
     * service notices no change since the previous iteration. Changes include additions of new tasks and additions of
     * new leases.
     */
    public void start() {
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                TaskSchedulingService.this.scheduleOnce();
            }
        }, 0, loopIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Mark this scheduler as shutdown and prevent any further scheduling iterations from starting. This may let an
     * already running scheduling iteration to complete.
     */
    public void shutdown() {
        executorService.shutdown();
    }

    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    private void scheduleOnce() {
        try {
            taskScheduler.checkIfShutdown();
        }
        catch (IllegalStateException e) {
            logger.warn("Shutting down due to taskScheduler being shutdown");
            shutdown();
            return;
        }
        try {
            // check if next scheduling iteration is actually needed right away
            final boolean qModified = taskQueue.reset();
            addPendingRunningTasks();
            removeTasks();
            final boolean newLeaseExists = leaseBlockingQueue.peek() != null;
            if ( qModified || newLeaseExists || doNextIteration()) {
                taskScheduler.setTaskToClusterAutoScalerMapGetter(taskToClusterAutoScalerMapGetter);
                lastSchedIterationAt.set(System.currentTimeMillis());
                if (preHook != null)
                    preHook.call();
                List<VirtualMachineLease> currentLeases = new ArrayList<>();
                leaseBlockingQueue.drainTo(currentLeases);
                final SchedulingResult schedulingResult = taskScheduler.scheduleOnce(taskQueue, currentLeases);
                // mark end of scheduling iteration before assigning tasks.
                taskQueue.getUsageTracker().reset();
                assignTasks(schedulingResult, taskScheduler);
                schedulingResultCallback.call(schedulingResult);
                doPendingActions();
            }
        }
        catch (Exception e) {
            // TODO return exception to scheduling result callback
            SchedulingResult result = new SchedulingResult(null);
            result.addException(e);
            schedulingResultCallback.call(result);
        }
    }

    private void addPendingRunningTasks() {
        // add any pending running tasks
        if (addRunningTasksQueue.peek() != null) {
            List<Map<String, QueuableTask>> r = new LinkedList<>();
            addRunningTasksQueue.drainTo(r);
            for (Map<String, QueuableTask> m: r) {
                for (Map.Entry<String, QueuableTask> entry: m.entrySet())
                    taskScheduler.getTaskAssignerIntl().call(entry.getValue(), entry.getKey());
            }
        }
    }

    private void removeTasks() {
        if (removeTasksQueue.peek() != null) {
            List<RemoveTaskRequest> l = new LinkedList<>();
            removeTasksQueue.drainTo(l);
            for (RemoveTaskRequest r: l) {
                // remove it from the queue and call taskScheduler to unassign it if hostname is not null
                try {
                    taskQueue.getUsageTracker().removeTask(r.taskId, r.qAttributes);
                } catch (TaskQueueException e1) {
                    // shouldn't happen since we're calling outside of scheduling iteration
                    logger.warn("Unexpected to get exception outside of scheduling iteration: " + e1.getMessage(), e1);
                }
                if (r.hostname != null)
                    taskScheduler.getTaskUnAssigner().call(r.taskId, r.hostname);
            }
        }
    }

    private void doPendingActions() {
        final Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>> action = taskMapRequest.poll();
        try {
            if (action != null)
                action.call(taskQueue.getAllTasks());
        } catch (TaskQueueException e) {
            logger.warn("Unexpected when trying to get task list: " + e.getMessage(), e);
        }
        final Action1<Map<String, Map<VMResource, Double[]>>> rsAction = resStatusRequest.poll();
        try {
            if (rsAction != null)
                rsAction.call(taskScheduler.getResourceStatusIntl());
        } catch (IllegalStateException e) {
            logger.warn("Unexpected when trying to get resource status: " + e.getMessage(), e);
        }
        final Action1<List<VirtualMachineCurrentState>> vmcAction = vmCurrStateRequest.poll();
        try {
            if (vmcAction != null)
                vmcAction.call(taskScheduler.getVmCurrentStatesIntl());
        } catch (IllegalStateException e) {
            logger.warn("Unexpected when trying to get vm current states: " + e.getMessage(), e);
        }
    }

    private boolean doNextIteration() {
        return (System.currentTimeMillis() - lastSchedIterationAt.get()) > maxSchedIterDelay;
    }

    private void assignTasks(SchedulingResult schedulingResult, TaskScheduler taskScheduler) {
        if(!schedulingResult.getResultMap().isEmpty()) {
            for (VMAssignmentResult result: schedulingResult.getResultMap().values()) {
                for (TaskAssignmentResult t: result.getTasksAssigned())
                    taskScheduler.getTaskAssignerIntl().call(t.getRequest(), result.getHostname());
            }
        }
    }

    /**
     * Add new leases to be used for next scheduling iteration. Leases with IDs previously added cannot be added
     * again. If duplicates are found, the scheduling iteration throws an exception and is available via the
     * scheduling result callback. See {@link TaskScheduler#scheduleOnce(List, List)} for details on behavior upon
     * encountering an exception. This method can be called anytime without impacting any currently running scheduling
     * iterations. The leases will be used in the next scheduling iteration.
     * @param leases New leases to use for scheduling.
     */
    public void addLeases(List<? extends VirtualMachineLease> leases) {
        if (leases != null && !leases.isEmpty()) {
            for(VirtualMachineLease l: leases)
                leaseBlockingQueue.offer(l);
        }
    }

    /**
     * Get all of the tasks in the scheduling service's queue and call the given action when it is available. The
     * list of queues returned is in a consistent state, that is, transitionary actions from ongoing scheduling
     * iterations do not affect the returned collection of tasks. Although an ongoing scheduling iteration is
     * unaffected by this call, onset of the next scheduling iteration may be delayed until the call to the given
     * {@code action} returns. Therefore, it is expected that the {@code action} callback return quickly.
     * @param action The action to call with task collection.
     * @throws TaskQueueException if too many actions are pending to get tasks collection.
     */
    public void requestAllTasks(Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>> action) throws TaskQueueException {
        if (!taskMapRequest.offer(action))
            throw new TaskQueueException("Too many pending actions submitted for getting tasks collection");
    }

    /**
     * Get resource status information and call the given action when available. Although an ongoing scheduling
     * iteration is unaffected by this call, onset of the next scheduling iteration may be delayed until the call to the
     * given {@code action} returns. Therefore, it is expected that the {@code action} callback return quickly.
     * @param action The action to call with resource status.
     * @throws TaskQueueException if too many actions are pending to get resource status.
     */
    public void requestResourceStatus(Action1<Map<String, Map<VMResource, Double[]>>> action) throws TaskQueueException {
        if (!resStatusRequest.offer(action))
            throw new TaskQueueException("Too many pending actions submitted for getting resource status");
    }

    /**
     * Get the current states of all known VMs and call the given action when available. Although an ongoing scheduling
     * iteration is unaffected by this call, onset of the next scheduling iteration may be delayed until the call to the
     * given {@code action} returns. Therefore, it is expected that the {@code action} callback return quickly.
     * @param action The action to call with VM states.
     * @throws TaskQueueException if too many actions are pending to get VM states.
     */
    public void requestVmCurrentStates(Action1<List<VirtualMachineCurrentState>> action) throws TaskQueueException {
        if (!vmCurrStateRequest.offer(action))
            throw new TaskQueueException("Too many pending actions submitted for getting VM current state");
    }

    /**
     * Mark the given tasks as running. This is expected to be called for all tasks that were already running from before
     * {@link com.netflix.fenzo.TaskSchedulingService} started running. For example, when the scheduling service
     * is being started after a restart of the system and there were some tasks launched in the previous run of
     * the system. Any tasks assigned resources during scheduling invoked by this service will be automatically marked
     * as running.
     * <P>
     * @param task The task to mark as running
     * @param hostname The name of the VM that the task is running on.
     */
    public void initializeRunningTask(QueuableTask task, String hostname) {
        addRunningTasksQueue.offer(Collections.singletonMap(hostname, task));
    }

    /**
     * Mark the task to be removed. This is expected to be called for all tasks that were added to the queue, whether or
     * not the task is already running. If the task is running, the <code>hostname</code> parameter must be set, otherwise,
     * it can be <code>null</code>. The actual remove operation is performed before the start of the next scheduling
     * iteration.
     * @param taskId The Id of the task to be removed.
     * @param qAttributes The queue attributes of the queue that the task belongs to
     * @param hostname The name of the VM where the task was assigned resources from, or, <code>null</code> if it was
     *                 not assigned any resources.
     */
    public void removeTask(String taskId, QAttributes qAttributes, String hostname) {
        removeTasksQueue.offer(new RemoveTaskRequest(taskId, qAttributes, hostname));
    }

    /**
     * Set the getter function that maps a given queuable task object to a list of names of VM groups for which
     * cluster autoscaling rules have been set. This function will be called by autoscaler, if it was setup for
     * the {@link TaskScheduler} using {@link TaskScheduler.Builder#withAutoScaleRule(AutoScaleRule)}, to determine if
     * the autoscaling rule should be triggered for aggressive scale up. The function call is expected to return a list
     * of autoscale group names to which the task can be launched, if there are resources available. If either this
     * function is not set, or if the function returns no entries when called, the task is assumed to be able to run
     * on any autoscale group.
     * @param getter The function that takes a queuable task object and returns a list of autoscale group names
     */
    public void setTaskToClusterAutoScalerMapGetter(Func1<QueuableTask, List<String>> getter) {
        taskToClusterAutoScalerMapGetter = getter;
    }

    public final static class Builder {

        private TaskScheduler taskScheduler = null;
        private Action1<SchedulingResult> schedulingResultCallback = null;
        private InternalTaskQueue taskQueue = null;
        private long loopIntervalMillis = 50;
        private final ScheduledExecutorService executorService;
        private Action0 preHook = null;
        private long maxDelayMillis = 5000L;

        public Builder() {
            executorService = new ScheduledThreadPoolExecutor(1);
        }

        /**
         * Use the given instance of {@link TaskScheduler} for scheduling resources. A task scheduler must be provided
         * before this builder can create the scheduling service.
         * @param taskScheduler The task scheduler instance to use.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskSchedulingService}.
         */
        public Builder withTaskScheduler(TaskScheduler taskScheduler) {
            this.taskScheduler = taskScheduler;
            return this;
        }

        /**
         * Use the given callback to give scheduling results to at the end of each scheduling iteration. A callback must
         * be provided before this builder can create the scheduling service.
         * @param callback The action to call with scheduling results.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskSchedulingService}.
         */
        public Builder withSchedulingResultCallback(Action1<SchedulingResult> callback) {
            this.schedulingResultCallback = callback;
            return this;
        }

        /**
         * Use the given instance of {@link com.netflix.fenzo.queues.TaskQueue} from which to get tasks to assign
         * resource to. A task queue must be provided before this builder can create the scheduling service.
         * @param taskQ The task queue from which to get tasks for assignment of resoruces.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskSchedulingService}.
         */
        public Builder withTaskQuue(TaskQueue taskQ) {
            if (!(taskQ instanceof InternalTaskQueue))
                throw new IllegalArgumentException("Argument is not a valid implementation of task queue");
            taskQueue = (InternalTaskQueue) taskQ;
            return this;
        }

        /**
         * Use the given milli seconds as minimum delay between two consecutive scheduling iterations. Default to 50.
         * @param loopIntervalMillis The delay between consecutive scheduling iterations in milli seconds.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskSchedulingService}.
         */
        public Builder withLoopIntervalMillis(long loopIntervalMillis) {
            this.loopIntervalMillis = loopIntervalMillis;
            return this;
        }

        /**
         * Use the given milli seconds as the maximum delay between two consecutive scheduling iterations. Default to
         * 5000. Delay between two iterations may be longer than the minimum delay specified using
         * {@link #withLoopIntervalMillis(long)} if the service notices no changes to the queue or there are no new
         * VM leases input.
         * @param maxDelayMillis The maximum delay between two consecutive scheduling iterations.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskSchedulingService}.
         */
        public Builder withMaxDelayMillis(long maxDelayMillis) {
            this.maxDelayMillis = maxDelayMillis;
            return this;
        }

        /**
         * Use the given action to call before starting a new scheduling iteration. This can be used, for example,
         * to prepare for the next iteration by updating any state that user provided plugins may wish to use for
         * constraints and fitness functions.
         * @param preHook The callback to mark beginning of a new scheduling iteration.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskSchedulingService}.
         */
        public Builder withPreSchedulingLoopHook(Action0 preHook) {
            this.preHook = preHook;
            return this;
        }

        /**
         * Creates a {@link TaskSchedulingService} based on the various builder methods you have chained.
         *
         * @return a {@code TaskSchedulingService} built according to the specifications you indicated
         */
        public TaskSchedulingService build() {
            if (taskScheduler == null)
                throw new NullPointerException("Null task scheduler not allowed");
            if (schedulingResultCallback == null)
                throw new NullPointerException("Null scheduling result callback not allowed");
            if (taskQueue == null)
                throw new NullPointerException("Null task queue not allowed");
            return new TaskSchedulingService(this);
        }
    }
}
