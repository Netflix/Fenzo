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

import com.netflix.fenzo.queues.Assignable;
import com.netflix.fenzo.queues.TaskQueueException;

public interface TaskIterator {
    /**
     * Get the next task from queue, or {@code null} if no more tasks exist.
     * @return The next task or a task with an assignment failure, if the task cannot be scheduled due to some
     *         internal constraints (for example exceeds allowed resource usage for a queue).
     *         Returns {@code null} if there are no tasks left to assign resources to.
     * @throws TaskQueueException if there were errors retrieving the next task from the queue.
     */
    Assignable<? extends TaskRequest> next() throws TaskQueueException;
}
