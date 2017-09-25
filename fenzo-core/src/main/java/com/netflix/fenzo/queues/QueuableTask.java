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

import com.netflix.fenzo.TaskRequest;

/**
 * A queuable task extends {@link TaskRequest} with a method to get attributes for the queue.
 */
public interface QueuableTask extends TaskRequest {

    /**
     * Get the attributes for the queue that the task belongs to.
     * @return The queue attributes for this task.
     */
    QAttributes getQAttributes();

    /**
     * Get the time at which this task is ready for consideration for assignment. This can be compared to system's
     * current time, for example, via {@link System#currentTimeMillis()}, to determine if the task is ready for being
     * considered for assignment. If the returned time is less than current time, then it is not ready. A return time
     * of <code>0</code> implies that the task is ready now. Tasks that are not ready in a scheduling iteration may
     * be skipped to be considered in the next scheduling iteration.
     * @return Time in milli seconds when this task is ready, or <code>0</code> to indicate it is ready.
     */
    default long getReadyAt() {
        return 0L;
    }
}
