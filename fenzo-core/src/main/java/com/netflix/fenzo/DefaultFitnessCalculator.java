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
 * A default fitness calculator that always finds any target to be perfectly fit for any task. A fitness
 * calculator computes * a value between 0.0 and 1.0 to indicate the confidence with which it believes a
 * particular target is suitable for a particular task, with 0.0 signifying that it is completely unconfident,
 * and 1.0 signifying that it is completely confident. This default fitness calculator always computes this
 * value to be 1.0, meaning that it always believes a target and a task are well matched for each other.
 */
public class DefaultFitnessCalculator implements VMTaskFitnessCalculator {
    public DefaultFitnessCalculator() {
    }

    /**
     * Returns the name of this fitness calculator (the class name).
     *
     * @return the name of this fitness calculator
     */
    @Override
    public String getName() {
        return DefaultFitnessCalculator.class.getName();
    }

    /**
     * Computes the suitability of {@code targetVM} to take on the task described by {@code taskRequest} to be
     * 1.0 (fully suitable).
     *
     * @param taskRequest a description of the task to be assigned
     * @param targetVM a description of the host to which the task may potentially be assigned
     * @param taskTrackerState the state of tasks and task assignments in the system at large
     * @return 1.0
     */
    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        return 1.0;
    }
}
