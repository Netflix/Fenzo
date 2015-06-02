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

    public Map<String, VMAssignmentResult> getResultMap() {
        return resultMap;
    }

    void addFailures(TaskRequest request, List<TaskAssignmentResult> f) {
        failures.put(request, f);
    }

    public Map<TaskRequest, List<TaskAssignmentResult>> getFailures() {
        return failures;
    }

    public int getLeasesAdded() {
        return leasesAdded;
    }

    void setLeasesAdded(int leasesAdded) {
        this.leasesAdded = leasesAdded;
    }

    public int getLeasesRejected() {
        return leasesRejected;
    }

    void setLeasesRejected(int leasesRejected) {
        this.leasesRejected = leasesRejected;
    }

    public long getRuntime() {
        return runtime;
    }

    void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public int getNumAllocations() {
        return numAllocations;
    }

    void setNumAllocations(int numAllocations) {
        this.numAllocations = numAllocations;
    }

    public int getTotalVMsCount() {
        return totalVMsCount;
    }

    void setTotalVMsCount(int totalVMsCount) {
        this.totalVMsCount = totalVMsCount;
    }

    public int getIdleVMsCount() {
        return idleVMsCount;
    }

    void setIdleVMsCount(int idleVMsCount) {
        this.idleVMsCount = idleVMsCount;
    }
}
