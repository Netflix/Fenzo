/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.fenzo.plugins;

import java.util.List;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;

/**
 * A fitness calculator that calculates the weighted average of multiple fitness calculators.
 */
public class WeightedAverageFitnessCalculator implements VMTaskFitnessCalculator {

    private final List<WeightedFitnessCalculator> calculators;

    public WeightedAverageFitnessCalculator(List<WeightedFitnessCalculator> calculators) {
        this.calculators = calculators;
        if (calculators == null || calculators.isEmpty()) {
            throw new IllegalArgumentException("There must be at least 1 calculator");
        }
        double sum = calculators.stream()
                .map(WeightedFitnessCalculator::getWeight)
                .mapToDouble(Double::doubleValue).sum();
        if (sum != 1.0) {
            throw new IllegalArgumentException("The sum of the weights must equal 1.0");
        }
    }

    @Override
    public String getName() {
        return toString();
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        double totalWeightedScores = 0.0;
        double totalWeights = 0.0;
        for (WeightedFitnessCalculator calculator : calculators) {
            double score = calculator.getFitnessCalculator().calculateFitness(taskRequest, targetVM, taskTrackerState);
            // If any of the scores are 0.0 then the final score should be 0.0
            if (score == 0.0) {
                return score;
            }
            totalWeightedScores += (score * calculator.getWeight());
            totalWeights += calculator.getWeight();
        }
        return totalWeightedScores / totalWeights;
    }

    @Override
    public String toString() {
        return "Weighted Average Fitness Calculator: " + calculators;
    }

    public static class WeightedFitnessCalculator {
        private final VMTaskFitnessCalculator fitnessCalculator;
        private final double weight;

        public WeightedFitnessCalculator(VMTaskFitnessCalculator fitnessCalculator, double weight) {
            if (fitnessCalculator == null) {
                throw new IllegalArgumentException("Fitness Calculator cannot be null");
            }
            this.fitnessCalculator = fitnessCalculator;
            this.weight = weight;
        }

        public VMTaskFitnessCalculator getFitnessCalculator() {
            return fitnessCalculator;
        }

        public double getWeight() {
            return weight;
        }

        @Override
        public String toString() {
            return "{ fitnessCalculator: " + fitnessCalculator.getName() + ", weight: " + weight + " }";
        }
    }
}
