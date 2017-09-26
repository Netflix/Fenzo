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

package com.netflix.fenzo.queues;

import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.sla.ResAllocs;

import java.util.Collection;
import java.util.Map;

/**
 * This interface represents a queue entity whose usage is tracked. Some of the methods in this class are called
 * during a scheduling iteration, and, therefore, implementations must be efficient so as to not slow down the
 * scheduler.
 * <P>
 * There are effectively two sets of methods. One set is called during a scheduling iteration. These are:
 * <UL>
 *     <LI>{@link #nextTaskToLaunch()}</LI>
 *     <LI>{@link #assignTask(QueuableTask)}</LI>
 * </UL>
 * The other set is called between consecutive scheduling iterations. Scheduling iteration is marked as completed
 * by calling {@link #reset()}. The methods in this set are:
 * <UL>
 *     <LI>{@link #queueTask(QueuableTask)}</LI>
 *     <LI>{@link #launchTask(QueuableTask)}</LI>
 *     <LI>{@link #removeTask(String, QAttributes)}</LI>
 * </UL>
 */
public interface UsageTrackedQueue {

    class ResUsage {
        private final ResAllocs resAllocsWrapper;

        private double cpus=0.0;
        private double memory=0.0;
        private double networkMbps=0.0;
        private double disk=0.0;

        public ResUsage() {
            resAllocsWrapper = new ResAllocs() {
                @Override
                public String getTaskGroupName() {
                    return "usage";
                }

                @Override
                public double getCores() {
                    return cpus;
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
            };
        }

        public void addUsage(QueuableTask task) {
            cpus += task.getCPUs();
            memory += task.getMemory();
            networkMbps += task.getNetworkMbps();
            disk += task.getDisk();
        }

        public void remUsage(QueuableTask task) {
            cpus -= task.getCPUs();
            memory -= task.getMemory();
            networkMbps -= task.getNetworkMbps();
            disk -= task.getDisk();
        }

        public ResAllocs getResAllocsWrapper() {
            return resAllocsWrapper;
        }

        public double getCpus() {
            return cpus;
        }

        public double getMemory() {
            return memory;
        }

        public double getNetworkMbps() {
            return networkMbps;
        }

        public double getDisk() {
            return disk;
        }

        public double getDominantResUsageFrom(ResAllocs totalResources) {
            double tCPU = totalResources.getCores();
            double max = tCPU > 0.0 ? cpus / tCPU : cpus;

            double tMemory = totalResources.getMemory();
            double tmp = tMemory > 0.0? memory / tMemory : memory;
            max = Math.max(max, tmp);

            double tNetwork = totalResources.getNetworkMbps();
            tmp = tNetwork > 0.0? networkMbps / tNetwork : networkMbps;
            max = Math.max(max, tmp);

            double tDisk = totalResources.getDisk();
            tmp = tDisk > 0.0? disk / tDisk : disk;
            max = Math.max(max, tmp);

            return max;
        }
    }

    /**
     * Add the given task to the queue. Tasks can be added to the queue only while the queue isn't being iterated
     * upon for a scheduling loop. If it is, then this method throws an exception.
     * @param t The task to add to the queue.
     * @throws TaskQueueException if either the task already exists in the queue or if the queue is being iterated on.
     */
    void queueTask(QueuableTask t) throws TaskQueueException;

    /**
     * Get the next task to assign resources to. This method is called from within a scheduling iteration to assign
     * resources to it. The scheduling iteration calls this method repeatedly, until a {@code null} is returned. The
     * first call to this method marks the queue as being iterated upon for scheduling. The {@link #reset()} method
     * must be called to mark the end of the scheduling iteration, after which other queue modification methods such as
     * {@link #queueTask(QueuableTask)}, {@link #launchTask(QueuableTask)}, and {@link #removeTask(String, QAttributes)}
     * can be called.
     * @return The next task or a task with an assignment failure, if the task cannot be scheduled due to some
     *         internal constraints (for example exceeds allowed resource usage for a queue).
     *         Returns {@code null} if there are no tasks left to assign resources to.
     * @throws TaskQueueException if there was an error getting next task from the queue.
     */
    Assignable<QueuableTask> nextTaskToLaunch() throws TaskQueueException;

    /**
     * Mark the given task to be assigned resources. Assignment is a step within a scheduling iteration. The resources
     * assigned to the task are committed from the perspective of total resource usage. This method can be called only
     * while a queue is being iterated upon, from within a scheduling iteration. Calling it outside of an iteration
     * results in an exception being thrown. A call to {@link #nextTaskToLaunch()} marks the queue as being iterated on.
     * @param t The task to be marked as assigned.
     * @throws TaskQueueException if this method was called outside of a scheduling loop.
     */
    void assignTask(QueuableTask t) throws TaskQueueException;

    /**
     * Mark the given task as launched. That is, the task is now sent to the agent for running, so the resources
     * are fully committed for usage until the task is removed from the queue. Tasks can be launched in two scenarios:
     * a) after assigning resources to all tasks, the scheduling loop ends and tasks are launched, or b) service is
     * initialized and tasks previously known to be running are marked as launched. Resource usage is tracked in the
     * queues, which is correctly updated only once for a task even if both {@link #assignTask(QueuableTask)} and
     * this method are called for the same task. This method indicates if the resource usage totals were updated
     * during this call.
     * @param t The task to launch.
     * @return True if resources for this task were actually added to total usage, false if not.
     * @throws TaskQueueException if the queue is being iterated on for a scheduling iteration.
     */
    boolean launchTask(QueuableTask t) throws TaskQueueException;

    /**
     * Remove the given task from the queue, irrespective of whether it is queued or launched. This cannot be called
     * while the queue is being iterated on, for example, in a scheduling loop. {@link #reset()} must be called before
     * calling this method.
     * @param id The task id to remove
     * @param qAttributes The queue attributes for the task to remove
     * @return {@link QueuableTask} that was removed, or {@code null} if the task wasn't found.
     * @throws TaskQueueException if the queue is being iterated on for a scheduling iteration.
     */
    QueuableTask removeTask(String id, QAttributes qAttributes) throws TaskQueueException;

    /**
     * Set the ready for the given task.
     * @see QueuableTask#getReadyAt()
     * @param taskId
     * @param qAttributes
     * @param when
     */
    void setTaskReadyTime(String taskId, QAttributes qAttributes, long when) throws TaskQueueException;

    /**
     * Get the usage of the dominant resource, expressed as a share of the total known available resources.
     * @return The dominant resource usage.
     */
    double getDominantUsageShare();

    /**
     * Reset the queue to mark the end of a scheduling iteration.
     */
    void reset();

    /**
     * Get list of all tasks grouped by their state. The list is expected to be consistent, without any transitionary
     * affects from an ongoing scheduling iteration. This must be called only from outside of a scheduling iteration.
     * @return List of all tasks in queue as a {@link Map} with {@link TaskQueue.TaskState} as key and {@link Collection} of
     * {@link QueuableTask} as values.
     * @throws TaskQueueException if called concurrently with a scheduling iteration in progress.
     */
    Map<TaskQueue.TaskState, Collection<QueuableTask>> getAllTasks() throws TaskQueueException;

    /**
     * Set the map of total resources available. This is required to evaluate the dominant resource share used that may
     * be used by some queue implementations for fair share purposes.
     * @param totalResourcesMap Map of total resources to set.
     */
    void setTotalResources(Map<VMResource, Double> totalResourcesMap);
}
