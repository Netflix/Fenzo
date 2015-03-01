package io.mantisrx.fenzo;

import java.util.List;
import java.util.Set;

public class VMAssignmentResult {
    private final String hostname;
    private final List<VirtualMachineLease> leasesUsed;
    private final Set<TaskAssignmentResult> tasksAssigned;

    public VMAssignmentResult(String hostname, List<VirtualMachineLease> leasesUsed, Set<TaskAssignmentResult> tasksAssigned) {
        this.hostname = hostname;
        this.leasesUsed = leasesUsed;
        this.tasksAssigned = tasksAssigned;
    }
    public String getHostname() {
        return hostname;
    }
    public List<VirtualMachineLease> getLeasesUsed() {
        return leasesUsed;
    }
    public Set<TaskAssignmentResult> getTasksAssigned() {
        return tasksAssigned;
    }
}
