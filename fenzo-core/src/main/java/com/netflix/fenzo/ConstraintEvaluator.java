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
 * @warn interface description missing
 */
public interface ConstraintEvaluator {
    /**
     * @warn class description missing
     */
    public static class Result {
        private final boolean isSuccessful;
        private final String failureReason;

        public Result(boolean successful, String failureReason) {
            isSuccessful = successful;
            this.failureReason = isSuccessful? "" : failureReason;
        }

        /**
         * @warn method description missing
         *
         * @return
         */
        public boolean isSuccessful() {
            return isSuccessful;
        }

        /**
         * @warn method description missing
         *
         * @return
         */
        public String getFailureReason() {
            return failureReason;
        }
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public String getName();

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     *
     * @param taskRequest
     * @param targetVM
     * @param taskTrackerState
     * @return
     */
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                           TaskTrackerState taskTrackerState);
}
