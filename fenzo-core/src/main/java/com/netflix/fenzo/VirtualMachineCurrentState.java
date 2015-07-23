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
 * Representation of the current state of the virtual machine being considered for task assignment. Use this in the
 * fitness calculator plugin to influence task placement optimizations.
 */
public interface VirtualMachineCurrentState {

    /**
     * Get name of the host whose current state is available.
     *
     * @return Name of the host.
     */
    public String getHostname();

    /**
     * Get VM lease object representing totals of resources from all available leases (offers) for current
     * scheduling trial.
     *
     * @return a lease object representing currently available resources
     */
    public VirtualMachineLease getCurrAvailableResources();

    /**
     * Get list of task assignment results so far in the current scheduling trial.
     *
     * @return Collection of tasks assigned in current scheduling iteration, but not launched yet
     */
    public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned();

    /**
     * Get list of tasks assigned already from before current scheduling trial started.
     *
     * @return Collection of tasks running on this VM
     */
    public Collection<TaskRequest> getRunningTasks();
}
