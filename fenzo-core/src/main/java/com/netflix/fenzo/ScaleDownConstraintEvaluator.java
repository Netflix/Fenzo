/*
 * Copyright 2017 Netflix, Inc.
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

import java.util.Optional;

/**
 * An evaluator that for each VM computes a score from 0.0 to 1.0, with 0.0 being the lowest, and 1.0 highest. Evaluation
 * equal to 0.0, means that the VM should not be terminated at all. To allow state sharing during evaluation of a
 * sequence of candidate VMs, an evaluator implementation specific context is provided on each invocation. A new
 * context value is returned as part of the {@link Result}.
 */
public interface ScaleDownConstraintEvaluator<CONTEXT> {

    /**
     * Evaluation result.
     */
    class Result<CONTEXT> {
        private final double score;
        private final Optional<CONTEXT> context;

        private Result(double score, Optional<CONTEXT> context) {
            this.score = score;
            this.context = context;
        }

        public double getScore() {
            return score;
        }

        public Optional<CONTEXT> getContext() {
            return context;
        }

        public static <CONTEXT> Result<CONTEXT> of(double score) {
            return new Result<>(score, Optional.empty());
        }

        public static <CONTEXT> Result<CONTEXT> of(double score, CONTEXT context) {
            return new Result<>(score, Optional.of(context));
        }
    }

    /**
     * Returns the name of the constraint evaluator.
     *
     * @return the name of the constraint evaluator
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Return score from 0.0 to 1.0 for a candidate VM to be terminating. The higher the score, the higher priority of
     * the VM to get terminated. Value 0.0 means that it should not be terminated.
     *
     * @param candidate candidate VM to be removed
     * @param context evaluator specific data, held for single evaluation cycle. Initial value is set to Optional.empty().
     *                Evaluator should return new context as part of {@link Result} result. The new state will be passed
     *                during subsequent method invocation.
     *
     * @return result combining VM score, and new context state
     */
    Result evaluate(VirtualMachineLease candidate, Optional<CONTEXT> context);
}
