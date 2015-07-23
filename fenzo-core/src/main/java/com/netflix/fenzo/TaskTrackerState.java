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

import java.util.Map;

/**
 * State of tracked tasks in the scheduler.
 */
public interface TaskTrackerState {
    /**
     * Get a map of all running tasks. The keys are the task IDs of active tasks. The values are the corresponding
     * active task objects containing information on the active task.
     *
     * @return Map of all known running tasks.
     */
    public Map<String, TaskTracker.ActiveTask> getAllRunningTasks();

    /**
     * Get a map of all tasks currently assigned during a scheduling trial, but running yet. The keys are the task IDs
     * of the assigned tasks. The values are the corresponding active task objects containing information on the
     * assigned tasks.
     *
     * @return Map of all assigned tasks.
     */
    public Map<String, TaskTracker.ActiveTask> getAllCurrentlyAssignedTasks();
}
