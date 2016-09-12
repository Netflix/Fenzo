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
import com.netflix.fenzo.queues.InternalTaskQueue;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.queues.TaskQueueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A task scheduling service that maintains a scheduling loop to continuously assign resources to tasks pending in
 * the queue. This service maintains a scheduling loop to assign resources to tasks in the queue created when
 * constructing this service. It calls {@link TaskScheduler#scheduleOnce(TaskIterator, List)} from within its
 * scheduling loop. And, therefore, it is not allowed to call that method directly when using this service. Users of
 * this service instead add tasks into this service's queue, which are held until they are assigned. Here's a typical
 * use of this service:
 * <UL>
 *     <LI>
 *         Build a {@link TaskScheduler} using its builder, {@link TaskScheduler.Builder}.
 *     </LI>
 *     <LI>
 *         Build this service using its builder, {@link TaskSchedulingService.Builder}, providing a queue implementation
 *         such as {@link com.netflix.fenzo.queues.tiered.TieredQueue}. Specify scheduling interval and other callbacks.
 *     </LI>
 *     <LI>
 *         Start the scheduling loop by calling {@link #start()}.
 *     </LI>
 *     <LI>
 *         Receive callbacks for scheduling result that give you back a {@link SchedulingResult} object. Note that it is
 *         no longer required to call {@link TaskScheduler#getTaskAssigner()} for tasks assigned in the result. This
 *         service assigns the tasks before making the result available to you via the callback.
 *     </LI>
 * </UL>
 */
public class TaskSchedulingService {
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulingService.class);
    private final TaskScheduler taskScheduler;
    private final Action1<SchedulingResult> schedulingResultCallback;
    private final InternalTaskQueue taskQueue;
    private final ScheduledExecutorService executorService;
    private final long loopIntervalMillis;
    private final Action0 preHook;
    private final BlockingQueue<VirtualMachineLease> leaseBlockingQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Action1<Map<TaskQueue.State, Collection<QueuableTask>>>> taskMapRequest = new LinkedBlockingQueue<>(10);

    public TaskSchedulingService(Builder builder) {
        taskScheduler = builder.taskScheduler;
        schedulingResultCallback = builder.schedulingResultCallback;
        taskQueue = builder.taskQueue;
        taskScheduler.getTaskTracker().setUsageTrackedQueue(taskQueue.getUsageTracker());
        executorService = builder.executorService;
        loopIntervalMillis = builder.loopIntervalMillis;
        preHook = builder.preHook;
    }

    public void start() {
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                TaskSchedulingService.this.scheduleOnce();
            }
        }, 0, loopIntervalMillis, TimeUnit.MILLISECONDS);
    }

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
            if (preHook != null)
                preHook.call();
            List<VirtualMachineLease> currentLeases = new ArrayList<>();
            leaseBlockingQueue.drainTo(currentLeases);
            taskQueue.reset(); // mark end of scheduling iteration to the queue
            final SchedulingResult schedulingResult = taskScheduler.scheduleOnce(taskQueue, currentLeases);
            taskQueue.getUsageTracker().reset();
            assignTasks(schedulingResult, taskScheduler);
            schedulingResultCallback.call(schedulingResult);
            final Action1<Map<TaskQueue.State, Collection<QueuableTask>>> action = taskMapRequest.poll();
            try {
                if (action != null)
                    action.call(taskQueue.getAllTasks());
            }
            catch (TaskQueueException e) {
                logger.warn("Unexpected when trying to get task list: " + e.getMessage(), e);
            }
        }
        catch (Exception e) {
            // TODO return exception to scheduling result callback
            SchedulingResult result = new SchedulingResult(null);
            result.addException(e);
            schedulingResultCallback.call(result);
        }
    }

    private void assignTasks(SchedulingResult schedulingResult, TaskScheduler taskScheduler) {
        if(!schedulingResult.getResultMap().isEmpty()) {
            for (VMAssignmentResult result: schedulingResult.getResultMap().values()) {
                for (TaskAssignmentResult t: result.getTasksAssigned())
                    taskScheduler.getTaskAssigner().call(t.getRequest(), result.getHostname());
            }
        }
    }

    public void addLeases(List<VirtualMachineLease> leases) {
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
     * {@code action} returns. Therefore, it is expected that the provided {@code action} return quickly.
     * @param action The action to call with task collection.
     * @throws TaskQueueException if too many actions are pending to get tasks collection.
     */
    public void getAllTasks(Action1<Map<TaskQueue.State, Collection<QueuableTask>>> action) throws TaskQueueException {
        if (!taskMapRequest.offer(action))
            throw new TaskQueueException("Too many pending actions submitted for getting tasks collection");
    }

    public final static class Builder {

        private TaskScheduler taskScheduler = null;
        private Action1<SchedulingResult> schedulingResultCallback = null;
        private InternalTaskQueue taskQueue = null;
        private long loopIntervalMillis = 50;
        private ScheduledExecutorService executorService = null;
        private Action0 preHook = null;

        public Builder withTaskScheduler(TaskScheduler taskScheduler) {
            this.taskScheduler = taskScheduler;
            return this;
        }

        public Builder withSchedulingResultCallback(Action1<SchedulingResult> callback) {
            this.schedulingResultCallback = callback;
            return this;
        }

        public Builder withTaskQuue(TaskQueue taskQ) {
            if (!(taskQ instanceof InternalTaskQueue))
                throw new IllegalArgumentException("Argument is not a valid implementation of task queue");
            taskQueue = (InternalTaskQueue) taskQ;
            return this;
        }

        public Builder withLoopIntervalMillis(long loopIntervalMillis) {
            this.loopIntervalMillis = loopIntervalMillis;
            return this;
        }

        public Builder withPreSchedulingLoopHook(Action0 preHook) {
            this.preHook = preHook;
            return this;
        }

        public TaskSchedulingService build() {
            if (taskScheduler == null)
                throw new NullPointerException("Null task scheduler not allowed");
            if (schedulingResultCallback == null)
                throw new NullPointerException("Null scheduling result callback not allowed");
            if (taskQueue == null)
                throw new NullPointerException("Null task queue not allowed");
            if (executorService == null) {
                executorService = new ScheduledThreadPoolExecutor(1);
            }
            return new TaskSchedulingService(this);
        }
    }
}
