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
