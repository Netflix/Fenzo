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
import java.util.Set;

/**
 * Result of resource assignments for a host (VM).
 */
public class VMAssignmentResult {
    private final String hostname;
    private final List<VirtualMachineLease> leasesUsed;
    private final Set<TaskAssignmentResult> tasksAssigned;

    public VMAssignmentResult(String hostname, List<VirtualMachineLease> leasesUsed, Set<TaskAssignmentResult> tasksAssigned) {
        this.hostname = hostname;
        this.leasesUsed = leasesUsed;
        this.tasksAssigned = tasksAssigned;
    }

    /**
     * Get the name of the host whose assignment results are available.
     *
     * @return Name of te host.
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Get ehe list of leases (offers) used in creating the resource assignments for tasks.
     *
     * @return List of leases.
     */
    public List<VirtualMachineLease> getLeasesUsed() {
        return leasesUsed;
    }

    /**
     * Get the set of tasks that are assigned resources from this host.
     *
     * @return Set of tasks.
     */
    public Set<TaskAssignmentResult> getTasksAssigned() {
        return tasksAssigned;
    }
}
