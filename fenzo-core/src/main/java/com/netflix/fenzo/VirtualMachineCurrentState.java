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
 * Represents the current state of the virtual machine being considered for task assignment.
 * This is made available to the fitness calculator plugin, who may use the information from this state
 * object to influence task placement optimizations
 */
public interface VirtualMachineCurrentState {
    public String getHostname();
    /**
     * Returns a VM lease object representing totals of resources from all available leases for current
     * scheduling run.
     * @return
     */
    public VirtualMachineLease getCurrAvailableResources();

    /**
     * Get list of task assignment results so far in the current scheduling run.
     * @return
     */
    public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned();

    /**
     * Get list of tasks assigned already from before current scheduling run started.
     * @return
     */
    public Collection<TaskRequest> getRunningTasks();
}
