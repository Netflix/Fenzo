package com.netflix.fenzo;

import java.util.List;
import java.util.Set;

public class AutoScalerInput {
    private final List<VirtualMachineLease> idleResourcesList;
    private final Set<TaskRequest> failedTasks;

    public AutoScalerInput(List<VirtualMachineLease> idleResources, Set<TaskRequest> failedTasks) {
        this.idleResourcesList= idleResources;
        this.failedTasks = failedTasks;
    }
    public List<VirtualMachineLease> getIdleResourcesList() {
        return idleResourcesList;
    }
    public Set<TaskRequest> getFailures() {
        return failedTasks;
    }
}
