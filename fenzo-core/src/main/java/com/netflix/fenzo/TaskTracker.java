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

import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueueException;
import com.netflix.fenzo.queues.UsageTrackedQueue;
import com.netflix.fenzo.sla.ResAllocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to keep track of task assignments.
 */
public class TaskTracker {

    static class TaskGroupUsage implements ResAllocs {
        private final String taskGroupName;
        private double cores=0.0;
        private double memory=0.0;
        private double networkMbps=0.0;
        private double disk=0.0;

        private TaskGroupUsage(String taskGroupName) {
            this.taskGroupName = taskGroupName;
        }

        @Override
        public String getTaskGroupName() {
            return taskGroupName;
        }

        @Override
        public double getCores() {
            return cores;
        }

        @Override
        public double getMemory() {
            return memory;
        }

        @Override
        public double getNetworkMbps() {
            return networkMbps;
        }

        @Override
        public double getDisk() {
            return disk;
        }

        void addUsage(TaskRequest task) {
            cores += task.getCPUs();
            memory += task.getMemory();
            networkMbps += task.getNetworkMbps();
            disk += task.getDisk();
        }

        void subtractUsage(TaskRequest task) {
            cores -= task.getCPUs();
            if(cores < 0.0) {
                logger.warn("correcting cores usage <0.0");
                cores=0.0;
            }
            memory -= task.getMemory();
            if(memory<0.0) {
                logger.warn("correcting memory usage<0.0");
                memory=0.0;
            }
            networkMbps -= task.getNetworkMbps();
            if(networkMbps<0.0) {
                logger.warn("correcting networkMbps usage<0.0");
                networkMbps=0.0;
            }
            disk -= task.getDisk();
            if(disk<0.0) {
                logger.warn("correcting disk usage<0.0");
                disk=0.0;
            }
        }
    }

    // TODO move this class out into its own class instead of being an inner class
    /**
     * An active task in the scheduler.
     */
    public static class ActiveTask {
        private TaskRequest taskRequest;
        private AssignableVirtualMachine avm;
        public ActiveTask(TaskRequest taskRequest, AssignableVirtualMachine avm) {
            this.taskRequest = taskRequest;
            this.avm = avm;
        }

        /**
         * Get the task request object associated with the active task.
         *
         * @return the task request object
         */
        public TaskRequest getTaskRequest() {
            return taskRequest;
        }

        /**
         * Get the totals resource offers associated with the host on which the task is active.
         *
         * @return the total resource offers for the host
         */
        public VirtualMachineLease getTotalLease() {
            return avm.getCurrTotalLease();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TaskTracker.class);
    private final Map<String, ActiveTask> runningTasks = new HashMap<>();
    private final Map<String, ActiveTask> assignedTasks = new HashMap<>();
    private final Map<String, TaskGroupUsage> taskGroupUsages = new HashMap<>();
    private UsageTrackedQueue usageTrackedQueue = null;

    // package scoped
    TaskTracker() {
    }

    /* package */ void setUsageTrackedQueue(UsageTrackedQueue t) {
        usageTrackedQueue = t;
    }

    boolean addRunningTask(TaskRequest request, AssignableVirtualMachine avm) {
        final boolean added = runningTasks.put(request.getId(), new ActiveTask(request, avm)) == null;
        if(added) {
            addUsage(request);
            if (usageTrackedQueue != null && request instanceof QueuableTask)
                try {
                    usageTrackedQueue.launchTask((QueuableTask)request);
                } catch (TaskQueueException e) {
                    // We don't expect this to happen since we call this only outside scheduling iteration
                    logger.warn("Unexpected: " + e.getMessage());
                }
        }
        return added;
    }

    boolean removeRunningTask(String taskId) {
        final ActiveTask removed = runningTasks.remove(taskId);
        if(removed != null) {
            final TaskRequest task = removed.getTaskRequest();
            final TaskGroupUsage usage = taskGroupUsages.get(task.taskGroupName());
            if(usage==null)
                logger.warn("Unexpected to not find usage for task group " + task.taskGroupName() +
                        " to unqueueTask usage of task " + task.getId());
            else
                usage.subtractUsage(task);
            if (usageTrackedQueue != null && removed.getTaskRequest() instanceof QueuableTask)
                try {
                    final QueuableTask queuableTask = (QueuableTask) removed.getTaskRequest();
                    usageTrackedQueue.removeTask(queuableTask.getId(), queuableTask.getQAttributes());
                } catch (TaskQueueException e) {
                    // We don't expect this to happen since we call this only outside scheduling iteration
                    logger.warn("Unexpected: " + e.getMessage());
                }
        }
        return removed != null;
    }

    Map<String, ActiveTask> getAllRunningTasks() {
        return Collections.unmodifiableMap(runningTasks);
    }

    boolean addAssignedTask(TaskRequest request, AssignableVirtualMachine avm) {
        final boolean assigned = assignedTasks.put(request.getId(), new ActiveTask(request, avm)) == null;
        if(assigned) {
            addUsage(request);
            if (usageTrackedQueue != null && request instanceof QueuableTask)
                try {
                    usageTrackedQueue.assignTask((QueuableTask) request);
                } catch (TaskQueueException e) {
                    // We don't expect this to happen since we call this only from within a scheduling iteration
                    logger.warn("Unexpected: " + e.getMessage());
                }
        }
        return assigned;
    }

    private void addUsage(TaskRequest request) {
        TaskGroupUsage usage = taskGroupUsages.get(request.taskGroupName());
        if(usage==null) {
            taskGroupUsages.put(request.taskGroupName(), new TaskGroupUsage(request.taskGroupName()));
            usage = taskGroupUsages.get(request.taskGroupName());
        }
        usage.addUsage(request);
    }

    void clearAssignedTasks() {
        for(ActiveTask t: assignedTasks.values())
            taskGroupUsages.get(t.getTaskRequest().taskGroupName()).subtractUsage(t.getTaskRequest());
        assignedTasks.clear();
    }

    Map<String, ActiveTask> getAllAssignedTasks() {
        return Collections.unmodifiableMap(assignedTasks);
    }

    TaskGroupUsage getUsage(String taskGroupName) {
        return taskGroupUsages.get(taskGroupName);
    }

    void setTotalResources(Map<VMResource, Double> totalResourcesMap) {
        if (usageTrackedQueue != null)
            usageTrackedQueue.setTotalResources(totalResourcesMap);
    }
}
