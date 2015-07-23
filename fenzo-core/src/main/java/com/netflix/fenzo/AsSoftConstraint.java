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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;

/**
 * Transform a constraint to a soft constraint. A constraint evaluation returns a boolean result of success or failure.
 * However, a soft constraint is expected to return the degree to which the constraint is being met, a fitness value in
 * other words. Currently, this class takes a simple approach and returns the degree of meeting the constraint as
 * {@code 0.0} when the constraint fails and {@code 1.0} when it succeeds.
 */
public class AsSoftConstraint {

    /**
     * Convert a constraint evaluator to a fitness calculator so it can be used as a soft constraint.
     *
     * @param c The constraint evaluator to covert.
     * @return Fitness calculator to be used as a soft constraint.
     */
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
