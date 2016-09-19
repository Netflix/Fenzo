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

import org.apache.mesos.Protos;

import java.util.Collection;
import java.util.Map;

/**
 * Represents the current state of the host (virtual machine) that Fenzo is considering for a task assignment.
 * A fitness calculator plugin may use the information from this state object to influence how it optimizes task
 * placement decisions.
 */
public interface VirtualMachineCurrentState {

    /**
     * Get the name of the host on which the virtual machine is running.
     *
     * @return the hostname
     */
    String getHostname();

    /**
     * Get a map of resource sets of the virtual machine.
     *
     * @return The map of resource sets
     */
    Map<String, PreferentialNamedConsumableResourceSet> getResourceSets();

    /**
     * Returns a VM lease object representing totals of resources from all available leases on this host for the
     * current scheduling run.
     *
     * @return a lease object that represents resources that are currently available on the host
     */
    VirtualMachineLease getCurrAvailableResources();

    /**
     * Get all offers for the VM that represent the available resources. There may be more than one offer over time
     * if Mesos master offered partial resources for the VM multiple times.
     * @return A collection of Mesos resource offers.
     */
    Collection<Protos.Offer> getAllCurrentOffers();

    /**
     * Get list of task assignment results for this host so far in the current scheduling run.
     *
     * @return a collection of tasks that the current scheduling iteration assigned to this host but that are
     *         not launched/executing yet
     */
    Collection<TaskAssignmentResult> getTasksCurrentlyAssigned();

    /**
     * Get a list of those tasks that had already been assigned to this host before the current scheduling run
     * started.
     *
     * @return a collection of the tasks running on this host
     */
    Collection<TaskRequest> getRunningTasks();

    /**
     * Returns the time until which the given host remains disabled.
     *
     * @return time until which the host will remain disabled or 0 if the host is enabled
     */
    long getDisabledUntil();
}
