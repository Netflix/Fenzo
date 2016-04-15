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

    public class NamedResourceSetRequest {
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

    /**
     * Get an identifier for this task request.
     *
     * @return a task identifier
     */
    public String getId();

    /**
     * Get the name of the task group that this task request belongs to.
     *
     * @return the name of the group
     */
    public String taskGroupName();

    /**
     * Get the number of CPUs requested by the task.
     *
     * @return how many CPUs the task is requesting
     */
    public double getCPUs();

    /**
     * Get the amount of memory in MBs requested by the task.
     *
     * @return how many MBs of memory the task is requesting
     */
    public double getMemory();

    /**
     * Get the network bandwidth in Mbps requested by the task.
     *
     * @return how many Mbps of network bandwidth the task is requesting
     */
    public double getNetworkMbps();

    /**
     * Get the disk space in MBs requested by the task.
     *
     * @return how much disk space in MBs the task is requesting
     */
    public double getDisk();

    /**
     * Get the number of ports requested by the task.
     *
     * @return how many ports the task is requesting
     */
    public int getPorts();

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
    public List<? extends ConstraintEvaluator> getHardConstraints();

    /**
     * Get a list of the soft constraints the task requests. Soft constraints need not be satisfied by a host
     * for the task scheduler to assign this task to that host, but hosts that satisfy the soft constraints are
     * preferred by the task scheduler over those that do not.
     *
     * @return a List of soft constraints
     */
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints();
}
