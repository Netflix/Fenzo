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
 * Interface that represents resource allocation limits for a task group. This limits the sum of resources used
 * by all tasks that have that task group name to the amounts provided.
 * <p>
 * You obtain an object that implements this method by means of the {@link ResAllocsBuilder}.
 *
 * @see <a href="https://github.com/Netflix/Fenzo/wiki/Resource-Allocation-Limits">Resource Allocation
 *      Limits</a>
 */
public interface ResAllocs {
    String getTaskGroupName();

    /**
     * Limits the number of cores the task group can use to the number returned from this method.
     *
     * @return the maximum number of cores
     */
    double getCores();

    /**
     * Limits the amount of memory the task group can use to the number of MB returned from this method
     *
     * @return the maximum amount of memory, in MB
     */
    double getMemory();

    /**
     * Limits the amount of bandwidth the task group can use to the number of megabits per second returned from 
     * this method.
     *
     * @return the maximum network bandwidth, in Mbps
     */
    double getNetworkMbps();

    /**
     * Limits the amount of disk space the task group can use to the number of MB returned from this method.
     *
     * @return the maximum disk space, in MB
     */
    double getDisk();

    /**
     * Returns the the resource allocations in a string representation.
     *
     * @return the resources as a string
     */
    default String getAsString() {
        return "{ cpu: " + getCores() +
                ", memory: " + getMemory() +
                ", disk: " + getDisk() +
                ", networkMbps: " + getNetworkMbps() + " }";
    }
}
