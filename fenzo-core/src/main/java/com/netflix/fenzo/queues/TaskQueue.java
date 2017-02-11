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


import com.netflix.fenzo.TaskIterator;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.functions.Action1;

import java.util.Collection;
import java.util.Map;

/**
 * This interface defines a task queue that contains all tasks that are either pending resource allocation or assigned
 * resources while continuing to run. When using task queues, tasks are input into Fenzo once. The tasks are then
 * maintained in the queue until the task is explicitly removed due to no longer requiring resource assignment, or if
 * the task completed for any reason.
 * <P>
 * Methods in this interface are expected to be called concurrently. For example, tasks may be added to or removed from
 * the queue while a scheduling iteration using this queue is in progress. Implementations must handle this.
 */
public interface TaskQueue extends TaskIterator {

    /**
     * Tasks in a queue are said to be in one of two states. The {@link #QUEUED} state represents tasks pending
     * resource assignment. Where as, the {@link #LAUNCHED} state represents tasks that have been assigned resources,
     * such tasks may be either already executing or pending launch. This is used primarily for conveying the state of
     * the tasks via the callback passed to {@link com.netflix.fenzo.TaskSchedulingService#requestAllTasks(Action1)}.
     */
    enum TaskState { QUEUED, LAUNCHED }

    /**
     * Add a task to the queue. Duplicates are not allowed, as in, a task request that has the same Id as another
     * existing element will be rejected. The added task will be assigned resources by a scheduler. To add a task
     * into Fenzo that is already running from before, use
     * {@link com.netflix.fenzo.TaskSchedulingService#initializeRunningTask(QueuableTask, String)}.
     * <P>
     * This operation is designed to be performed asynchronously, when it is safe to modify the queue. The queue
     * implementations generally do not modify the queue while a scheduling iteration is in progress.
     * @param task A task to add to the queue.
     */
    void queueTask(QueuableTask task);

    /**
     * Set SLA for the queue. The queue implementation determines the implementation of {@link TaskQueueSla} that is
     * accepted.
     * @param sla The SLA to set for the queue.
     * @throws IllegalArgumentException if the implementation of the {@link TaskQueueSla} is incompatible with the
     * queue implementation.
     */
    void setSla(TaskQueueSla sla) throws IllegalArgumentException;
}
