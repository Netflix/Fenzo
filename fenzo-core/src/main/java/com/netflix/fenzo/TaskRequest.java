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

import java.util.List;

/**
 * A task request needing resource assignment.
 */
public interface TaskRequest {
    public String getId();

    /**
     * Get the name of the group that the task belongs to.
     *
     * @return Name of the group.
     */
    public String taskGroupName();

    /**
     * Get the number of CPUs requested by the task.
     *
     * @return Number of CPUs.
     */
    public double getCPUs();

    /**
     * Get the amount of memory in MBs requested by the task.
     *
     * @return Number of MBs of memory.
     */
    public double getMemory();

    /**
     * Get the network bandwidth in Mbps requested by the task.
     *
     * @return Network Mbps requested.
     */
    public double getNetworkMbps();

    /**
     * Get the disk space in MBs requested by the task
     *
     * @return The disk space in MBs.
     */
    public double getDisk();

    /**
     * Get the number of ports requested by the task.
     *
     * @return The number of ports.
     */
    public int getPorts();

    /**
     * Get list of hard constraints set by the task. All hard constraints must be met for assignment to succeed.
     *
     * @return List of hard constraints.
     */
    public List<? extends ConstraintEvaluator> getHardConstraints();

    /**
     * Get list of soft constraints set by the task. Soft constraints need not be satisfied for assignment to succeed,
     * but, generally, hosts that satisfy the constraints are preferred over those that don't.
     *
     * @return List of soft constraints.
     */
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints();
}
