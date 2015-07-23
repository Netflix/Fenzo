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
 * An interface to represent a task constraint evaluator.
 */
public interface ConstraintEvaluator {
    /**
     * Result of a task constraint evaluation.
     * Implementations of the interface are expected to indicate success or failure of constraint evaluation as well as
     * a message upon failure.
     */
    public static class Result {
        private final boolean isSuccessful;
        private final String failureReason;

        public Result(boolean successful, String failureReason) {
            isSuccessful = successful;
            this.failureReason = isSuccessful? "" : failureReason;
        }

        /**
         * Whether or not constraint evaluation was successful.
         *
         * @return {@code true} if successful, {@code false} otherwise.
         */
        public boolean isSuccessful() {
            return isSuccessful;
        }

        /**
         * Get failure reason.
         *
         * @return Failure message.
         */
        public String getFailureReason() {
            return failureReason;
        }
    }

    public String getName();

    /**
     * Evaluate constraint for a task being considered for assignment on a VM.
     *
     * @param taskRequest The task being considered for assignment.
     * @param targetVM The VM on which the task is being considered for assignment.
     * @param taskTrackerState The current state of all known tasks.
     * @return Result of constraint including whether or not successful along with possible failure reason.
     */
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                           TaskTrackerState taskTrackerState);
}
