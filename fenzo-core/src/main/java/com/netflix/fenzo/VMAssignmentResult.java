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
 * Result of resource assignments for a host (VM). When you call your task scheduler's
 * {@link TaskScheduler#scheduleOnce(java.util.List,java.util.List) scheduleOnce()} method to schedule a set of
 * tasks, that method returns a {@link SchedulingResult}. That object includes the method
 * {@link SchedulingResult#getResultMap() getResultMap()} which returns a map of host names to
 * {@code VMAssignmentResult} objects. Those objects in turn have the {@link #getLeasesUsed()} and
 * {@link #getTasksAssigned()} methods, which return information about the resource offers that participated in
 * the assignments and which tasks were assigned on those hosts, in the form of {@link VirtualMachineLease}
 * objects and {@link TaskAssignmentResult} objects respectively. This approach will give you insight into which
 * tasks have been assigned to which hosts in the current scheduling round (but not about which tasks are
 * already running on those hosts).
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
     * @return the name of the host
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Get the list of leases (resource offers) used in creating the resource assignments for tasks.
     *
     * @return a List of leases
     */
    public List<VirtualMachineLease> getLeasesUsed() {
        return leasesUsed;
    }

    /**
     * Get the set of tasks that are assigned resources from this host.
     *
     * @return a Set of task assignment results
     */
    public Set<TaskAssignmentResult> getTasksAssigned() {
        return tasksAssigned;
    }

    @Override
    public String toString() {
        return "VMAssignmentResult{" +
                "hostname='" + hostname + '\'' +
                ", leasesUsed=" + leasesUsed +
                ", tasksAssigned=" + tasksAssigned +
                '}';
    }
}
