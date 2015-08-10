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

/**
 * A constraint evaluator inspects a target to decide whether or not its attributes are satisfactory according
 * to some standard given the current state of task assignments in the system at large. Different constraint
 * evaluators look at different attributes and have different standards for how they make this decision.
 */
public interface ConstraintEvaluator {
    /**
     * The result of the evaluation of a {@link ConstraintEvaluator}. This combines a boolean that indicates
     * whether or not the target satisfied the constraint, and, if it did not, a string that includes the reason
     * why.
     */
    public static class Result {
        private final boolean isSuccessful;
        private final String failureReason;

        public Result(boolean successful, String failureReason) {
            isSuccessful = successful;
            this.failureReason = isSuccessful? "" : failureReason;
        }

        /**
         * Indicates whether the constraint evaluator found the target to satisfy the constraint.
         *
         * @return {@code true} if the target satisfies the constraint, {@code false} otherwise
         */
        public boolean isSuccessful() {
            return isSuccessful;
        }

        /**
         * Returns the reason why the target did not satisfy the constraint.
         *
         * @return the reason why the constraint was not satisfied, or an empty string if it was met
         */
        public String getFailureReason() {
            return failureReason;
        }
    }

    /**
     * Returns the name of the constraint evaluator.
     *
     * @return the name of the constraint evaluator
     */
    public String getName();

    /**
     * Inspects a target to decide whether or not it meets the constraints appropriate to a particular task.
     *
     * @param taskRequest a description of the task to be assigned
     * @param targetVM a description of the host that is a potential match for the task
     * @param taskTrackerState the current status of tasks and task assignments in the system at large
     * @return a successful Result if the target meets the constraints enforced by this constraint evaluator, or
     *         an unsuccessful Result otherwise
     */
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                           TaskTrackerState taskTrackerState);
}
