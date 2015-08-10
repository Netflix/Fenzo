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
 * Converts a hard constraint into a soft constraint. A {@link ConstraintEvaluator} is by default a "hard," or
 * mandatory constraint. Fenzo will not place a task with a target that fails such a constraint, and will not
 * place the task at all if no target passes the constraint. You can convert such a "hard" constraint into a
 * "soft" constraint by passing it into the {@link #get} method of this class. That method returns a
 * {@link VMTaskFitnessCalculator} that implements a "soft" constraint. When Fenzo uses such a constraint, it
 * will attempt to place a task with a target that satisfies the constraint, but will place the task with a
 * target that fails the constraint if no other target can be found.
 * <p>
 * The resulting "soft" constraint will return evaluate a host/task combination as 0.0 if the underlying hard
 * constraint would be violated, or 1.0 if the underlying hard constraint would be satisfied.
 * <p>
 * Note that some hard constraints may implement their own hard-to-soft conversion methods and that these
 * methods may return a soft constraint that is more nuanced, returning values between 0.0 and 1.0 and not just
 * those two values at either extreme (see, for example,
 * {@link com.netflix.fenzo.plugins.BalancedHostAttrConstraint BalancedHostAttrConstraint}).
 */
public class AsSoftConstraint {

    /**
     * Returns a "soft" constraint, in the form of a {@link VMTaskFitnessCalculator}, based on a specified "hard"
     * constraint, in the form of a {@link ConstraintEvaluator}.
     *
     * @param c the "hard" constraint to convert
     * @return a "soft" constraint version of {@code c}
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
