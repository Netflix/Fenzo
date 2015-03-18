package com.netflix.fenzo;

public class DefaultFitnessCalculator implements VMTaskFitnessCalculator {
    public DefaultFitnessCalculator() {
    }

    @Override
    public String getName() {
        return DefaultFitnessCalculator.class.getName();
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        return 1.0;
    }
}
