package com.netflix.fenzo;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;

public class AsSoftConstraint {
    public static VMTaskFitnessCalculator get(final ConstraintEvaluator c) {
        // This fitness calculator return 0 or 1. This can possibly be improved upon by the ConstraintEvaluator using its
        // own logic.
        return new VMTaskFitnessCalculator() {
            @Override
            public String getName() {
                return c.getName();
            }
            @Override
            public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                return c.evaluate(taskRequest, targetVM, taskTrackerState).isSuccessful()? 1.0 : 0.0;
            }
        };
    }
}
