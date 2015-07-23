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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of a scheduling trial, returned by {@link TaskScheduler#scheduleOnce(List, List)}.
 */
public class SchedulingResult {
    private final Map<String, VMAssignmentResult> resultMap;
    private final Map<TaskRequest, List<TaskAssignmentResult>> failures;
    private int leasesAdded;
    private int leasesRejected;
    private long runtime;
    private int numAllocations;
    private int totalVMsCount;
    private int idleVMsCount;

    SchedulingResult(Map<String, VMAssignmentResult> resultMap) {
        this.resultMap = resultMap;
        failures = new HashMap<>();
    }

    /**
     * Get the result map. The keys are host names on which at least one task has been assigned resources. The values
     * are the assignment results containing the offers used and assigned tasks.
     *
     * @return Map of results.
     */
    public Map<String, VMAssignmentResult> getResultMap() {
        return resultMap;
    }

    void addFailures(TaskRequest request, List<TaskAssignmentResult> f) {
        failures.put(request, f);
    }

    /**
     * Get assignment failures map incurred during scheduling. The keys are the task requests that failed assignments
     * and the values are the list of all failures incurred.
     *
     * @return Map of failures for all tasks that failed assignments.
     */
    public Map<TaskRequest, List<TaskAssignmentResult>> getFailures() {
        return failures;
    }

    /**
     * Get the number of leases (resource offers) added during the assignment trial.
     *
     * @return Number of leases added.
     */
    public int getLeasesAdded() {
        return leasesAdded;
    }

    void setLeasesAdded(int leasesAdded) {
        this.leasesAdded = leasesAdded;
    }

    /**
     * Get the number of leases (offers) rejected during the scheduling trial.
     *
     * @return Number of leases rejected.
     */
    public int getLeasesRejected() {
        return leasesRejected;
    }

    void setLeasesRejected(int leasesRejected) {
        this.leasesRejected = leasesRejected;
    }

    /**
     * Get the time taken, in milli seconds, for this scheduling trial.
     *
     * @return Time to complete the scheduling trial, in milli seconds.
     */
    public long getRuntime() {
        return runtime;
    }

    void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    /**
     * Get the number of resource allocation trials performed during the scheduling iteration.
     *
     * @return The number of resource allocations during this scheduling trial.
     */
    public int getNumAllocations() {
        return numAllocations;
    }

    void setNumAllocations(int numAllocations) {
        this.numAllocations = numAllocations;
    }

    /**
     * Get the total number of hosts (VMs) known during this scheduling iteration.
     *
     * @return The number of hosts (VMs) known.
     */
    public int getTotalVMsCount() {
        return totalVMsCount;
    }

    void setTotalVMsCount(int totalVMsCount) {
        this.totalVMsCount = totalVMsCount;
    }

    /**
     * Get the number of hosts (VMs) that are known to be idle at the end of this scheduling trial. This is the number
     * before any scale down action is triggered.
     *
     * @return The number of idle hosts.
     */
    public int getIdleVMsCount() {
        return idleVMsCount;
    }

    void setIdleVMsCount(int idleVMsCount) {
        this.idleVMsCount = idleVMsCount;
    }
}
