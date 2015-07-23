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

package com.netflix.fenzo.sla;

/**
 * Interface that represents resource allocations for a task group name.
 * Limit the sum of resources used by all tasks of the task group name to the amounts provided.
 */
public interface ResAllocs {
    String getTaskGroupName();

    /**
     * Get the number of cores to limit usage by all tasks of the group name.
     *
     * @return Total number of cores limit.
     */
    double getCores();

    /**
     * Get the amount of memory to limit usage by all tasks of the group name.
     *
     * @return Total memory limit.
     */
    double getMemory();

    /**
     * Get the amount of network bandwidth to limit usage by all tasks of the group name.
     *
     * @return Total network bandwidth limit.
     */
    double getNetworkMbps();

    /**
     * Get the amount of disk space to limit usage by all tasks of the group name.
     *
     * @return Total disk space limit.
     */
    double getDisk();
}
