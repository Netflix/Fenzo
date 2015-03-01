package io.mantisrx.fenzo;

public interface VMTaskFitnessCalculator {
    public String getName();
    /**
     * This is called by TaskScheduler during a scheduler run after a task's resource requirements are met by
     * a VirtualMachineCurrentState.
     * @param taskRequest  The task whose resource requirements can be met by the Virtual Machine.
     * @param targetVM     The prospective target Virtual Machine for given {@code taskRequest}.
     * @param taskTrackerState State of the task tracker that contains all tasks currently running and assigned
     * @return A value between 0.0 and 1.0, with higher values representing better fit of the task on the virtual machine.
     */
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                                   TaskTrackerState taskTrackerState);
}
