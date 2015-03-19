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
