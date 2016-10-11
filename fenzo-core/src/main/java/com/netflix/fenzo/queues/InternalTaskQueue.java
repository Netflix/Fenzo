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

import java.util.Collection;
import java.util.Map;

/**
 * This interface defines the behavior of task queues required for Fenzo internally. Implementations of TaskQueue must
 * implement this interface to be usable by {@link com.netflix.fenzo.TaskScheduler}.
 * <P>
 * Methods in this interface are expected to be called concurrently. For example, tasks may be added to or removed from
 * the queue while a scheduling iteration using this queue is in progress. Implementations must handle this. Note that,
 * it may not be sufficient for the implementations to use concurrent versions of collection classes for queue of tasks.
 * The queue must be consistent throughout the scheduling iteration. One recommended way to achieve such consistency is
 * to place the {@link #queueTask(QueuableTask)} operations as requests in a holding area within the implementation and
 * return immediately. Later, actually carry them out during the {@link #reset()} method.
 */
public interface InternalTaskQueue extends TaskQueue {
    /**
     * Reset the queue and make it ready for next scheduling iteration. Any operations requested that were not safe
     * to carry out during a scheduling iteration can be carried out during this method, before the next
     * scheduling iteration begins.
     * @return {@code true} if the queue was changed as part of this operation, {@code false} otherwise. The queue is
     * deemed changed if any queue modifications that were held for safety are carried out during this method, such as
     * adding to or removing from the queue.
     * @throws TaskQueueMultiException If any exceptions that may have occurred during resetting the pointer to the head
     * of the queue. Or, this may include exceptions that arose when applying any deferred operations from
     * {@link #queueTask(QueuableTask)} method.
     */
    boolean reset() throws TaskQueueMultiException;

    /**
     * Get the usage tracker, if any. Queue implementations may request updates for usage tracking purposes. If
     * provided, then {@link com.netflix.fenzo.TaskScheduler} will call the appropriate methods of the tracker
     * as scheduling assignments are taking place. This can help the queue implementations, for example, in maintaining
     * any fairness for resource usage across multiple entities within the queue. If this method returns
     * {@code null}, then the scheduler will ignore usage tracking for this queue.
     * <P>
     * Note that the implementations of {@link UsageTrackedQueue} must be efficient since they are called from
     * within the scheduling iteration.
     * @return The object to use for calling usage tracking triggers.
     */
    UsageTrackedQueue getUsageTracker();

    /**
     * Get all of the tasks in the queue. Consistent state of the queue is returned when this is called outside of
     * scheduling iteration runs. Calling this concurrently with a scheduling iteration results in an exception.
     * @return List of all tasks in queue as a {@link Map} with {@link TaskState} as key and {@link Collection} of
     * {@link QueuableTask} as values.
     * @throws TaskQueueException when called concurrently with a scheduling iteration in progress.
     */
    Map<TaskState, Collection<QueuableTask>> getAllTasks() throws TaskQueueException;
}
