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

import java.util.Collection;

/**
 * Represents the current state of the virtual machine that Fenzo is considering for a task assignment. The
 * fitness calculator plugin may use the information from this state object to influence how it optimizes task
 * placement decisions.
 */
public interface VirtualMachineCurrentState {

    /**
     * Get the name of the host on which the virtual machine is running.
     *
     * @return the hostname
     */
    public String getHostname();

    /**
     * Returns a VM lease object representing totals of resources from all available leases for the current
     * scheduling run.
     *
     * @return a lease object that represents resources that are currently available on the VM
     */
    public VirtualMachineLease getCurrAvailableResources();

    /**
     * Get list of task assignment results so far in the current scheduling run.
     *
     * @return a collection of tasks that the current scheduling iteration assigned but that are not
     *         launched/executing yet
     */
    public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned();

    /**
     * Get a list of those tasks that had already been assigned to this VM before the current scheduling run
     * started.
     *
     * @return a collection of the tasks running on this VM
     */
    public Collection<TaskRequest> getRunningTasks();
}
