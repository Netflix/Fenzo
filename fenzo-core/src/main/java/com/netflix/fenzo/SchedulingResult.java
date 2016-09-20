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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The result of a scheduling trial, as returned by
 * {@link TaskScheduler#scheduleOnce(List, List) scheduleOnce()}.
 * <p>
 * You can use this object to get a map of which tasks the task scheduler assigned to which virtual machines,
 * and then you can use this assignment map to launch the assigned tasks via Mesos.
 * <p>
 * You can also use the list of task assignment failures that is available through this object to make an
 * additional attempt to launch those tasks on your next scheduling loop or to modify the state of the system
 * to make it more amenable to the failed tasks.
 */
public class SchedulingResult {
    private final Map<String, VMAssignmentResult> resultMap;
    private final Map<TaskRequest, List<TaskAssignmentResult>> failures;
    private final List<Exception> exceptions;
    private int leasesAdded;
    private int leasesRejected;
    private long runtime;
    private int numAllocations;
    private int totalVMsCount;
    private int idleVMsCount;

    public SchedulingResult(Map<String, VMAssignmentResult> resultMap) {
        this.resultMap = resultMap;
        failures = new HashMap<>();
        exceptions = new ArrayList<>();
    }

    /**
     * Get the successful task assignment result map. The map keys are host names on which at least one task has
     * been assigned resources during the current scheduling round. The map values are the assignment results
     * that contain the offers used and assigned tasks.
     * <p>
     * Fenzo removes these offers from its internal state when you get this result. Normally, you would use these offers
     * immediately to launch the tasks. For any reason if you do not use the offers to launch those tasks, you must either
     * reject the offers to Mesos, or, re-add them to Fenzo with the next call to
     * {@link TaskScheduler#scheduleOnce(List, List)}. Otherwise, those offers would be "leaked out".
     *
     * @return a Map of the successful task assignments the task scheduler made in this scheduling round
     */
    public Map<String, VMAssignmentResult> getResultMap() {
        return resultMap;
    }

    void addFailures(TaskRequest request, List<TaskAssignmentResult> f) {
        failures.put(request, f);
    }

    public void addException(Exception e) {
        exceptions.add(e);
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    /**
     * Get the unsuccessful task assignment result map. The map keys are the task requests that the task
     * scheduler was unable to assign. The map values are a List of all of the failures that prevented the
     * task scheduler from assigning the task.
     *
     * @return a Map of the tasks the task scheduler failed to assign in this scheduling round
     */
    public Map<TaskRequest, List<TaskAssignmentResult>> getFailures() {
        return failures;
    }

    /**
     * Get the number of leases (resource offers) added during this scheduling trial.
     *
     * @return the number of leases added
     */
    public int getLeasesAdded() {
        return leasesAdded;
    }

    void setLeasesAdded(int leasesAdded) {
        this.leasesAdded = leasesAdded;
    }

    /**
     * Get the number of leases (resource offers) rejected during this scheduling trial.
     *
     * @return the number of leases rejected
     */
    public int getLeasesRejected() {
        return leasesRejected;
    }

    void setLeasesRejected(int leasesRejected) {
        this.leasesRejected = leasesRejected;
    }

    /**
     * Get the time elapsed, in milliseconds, during this scheduling trial.
     *
     * @return the time taken to complete this scheduling trial, in milliseconds
     */
    public long getRuntime() {
        return runtime;
    }

    void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    /**
     * Get the number of resource allocations performed during this scheduling trial.
     *
     * @return the number of resource allocations during this scheduling trial
     */
    public int getNumAllocations() {
        return numAllocations;
    }

    void setNumAllocations(int numAllocations) {
        this.numAllocations = numAllocations;
    }

    /**
     * Get the total number of hosts (virtual machines) known during this scheduling trial.
     *
     * @return the number of known hosts (VMs)
     */
    public int getTotalVMsCount() {
        return totalVMsCount;
    }

    void setTotalVMsCount(int totalVMsCount) {
        this.totalVMsCount = totalVMsCount;
    }

    /**
     * Get the number of hosts (virtual machines) that are idle at the end of this scheduling trial. This is the
     * number before any scale down action is triggered.
     *
     * @return the number of idle hosts
     */
    public int getIdleVMsCount() {
        return idleVMsCount;
    }

    void setIdleVMsCount(int idleVMsCount) {
        this.idleVMsCount = idleVMsCount;
    }

    @Override
    public String toString() {
        return "SchedulingResult{" +
                "resultMap=" + resultMap +
                ", failures=" + failures +
                ", leasesAdded=" + leasesAdded +
                ", leasesRejected=" + leasesRejected +
                ", runtime=" + runtime +
                ", numAllocations=" + numAllocations +
                ", totalVMsCount=" + totalVMsCount +
                ", idleVMsCount=" + idleVMsCount +
                '}';
    }
}
