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
import java.util.Map;

/**
 * Describes a task to be assigned to a host and its requirements.
 */
public interface TaskRequest {

    class NamedResourceSetRequest {
        private final String resName;
        private final String resValue;
        private final int numSets;
        private final int numSubResources;

        public NamedResourceSetRequest(String resName, String resValue, int numSets, int numSubResources) {
            this.resName = resName;
            this.resValue = resValue;
            this.numSets = numSets;
            this.numSubResources = numSubResources;
        }

        public String getResName() {
            return resName;
        }

        public String getResValue() {
            return resValue;
        }

        public int getNumSets() {
            return numSets;
        }

        public int getNumSubResources() {
            return numSubResources;
        }
    }

    class AssignedResources {
        private List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumedNamedResources;

        public List<PreferentialNamedConsumableResourceSet.ConsumeResult> getConsumedNamedResources() {
            return consumedNamedResources;
        }

        public void setConsumedNamedResources(List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumedNamedResources) {
            this.consumedNamedResources = consumedNamedResources;
        }
    }

    /**
     * Get an identifier for this task request.
     *
     * @return a task identifier
     */
    String getId();

    /**
     * Get the name of the task group that this task request belongs to.
     *
     * @return the name of the group
     */
    String taskGroupName();

    /**
     * Get the number of CPUs requested by the task.
     *
     * @return how many CPUs the task is requesting
     */
    double getCPUs();

    /**
     * Get the amount of memory in MBs requested by the task.
     *
     * @return how many MBs of memory the task is requesting
     */
    double getMemory();

    /**
     * Get the network bandwidth in Mbps requested by the task.
     *
     * @return how many Mbps of network bandwidth the task is requesting
     */
    double getNetworkMbps();

    /**
     * Get the disk space in MBs requested by the task.
     *
     * @return how much disk space in MBs the task is requesting
     */
    double getDisk();

    /**
     * Get the number of ports requested by the task.
     *
     * @return how many ports the task is requesting
     */
    int getPorts();

    /**
     * Get the scalar resources being requested by the task.
     * Although the cpus, memory, networkMbps, and disk are scalar resources, Fenzo currently treats the separately. Use
     * this scalar requests collection to specify scalar resources other than those four.
     * @return A {@link Map} of scalar resources requested, with resource name as the key and amount requested as the value.
     */
    Map<String, Double> getScalarRequests();

    /**
     * Get the list of custom named resource sets requested by the task.
     *
     * @return List of named resource set requests, or null.
     */
    Map<String, NamedResourceSetRequest> getCustomNamedResources();

    /**
     * Get a list of the hard constraints the task requires. All of these hard constraints must be satisfied by
     * a host for the task scheduler to assign this task to that host.
     *
     * @return a List of hard constraints
     */
    List<? extends ConstraintEvaluator> getHardConstraints();

    /**
     * Get a list of the soft constraints the task requests. Soft constraints need not be satisfied by a host
     * for the task scheduler to assign this task to that host, but hosts that satisfy the soft constraints are
     * preferred by the task scheduler over those that do not.
     *
     * @return a List of soft constraints
     */
    List<? extends VMTaskFitnessCalculator> getSoftConstraints();

    /**
     * Set the assigned resources for this task. These are resources other than the supported resources such as
     * CPUs, Memory, etc., and any scalar resources. This will specifically need to be set for tasks that are being
     * added into Fenzo as running from a prior instance of the scheduler running, for example, after the framework
     * hosting this instance of Fenzo restarts. That is, they were not assigned by this instance of Fenzo.
     * @param assignedResources The assigned resources to set for this task.
     * @see {@link AssignedResources}
     */
    void setAssignedResources(AssignedResources assignedResources);

    AssignedResources getAssignedResources();
}
