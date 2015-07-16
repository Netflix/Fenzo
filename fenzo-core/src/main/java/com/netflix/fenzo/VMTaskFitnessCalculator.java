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
public interface VMTaskFitnessCalculator {
    /**
     * @warn method description missing
     *
     * @return
     */
    public String getName();

    /**
     * This is called by {@code TaskScheduler} during a scheduler run after a task's resource requirements are
     * met by a {@code VirtualMachineCurrentState}.
     *
     * @param taskRequest      the task whose resource requirements can be met by the Virtual Machine
     * @param targetVM         the prospective target Virtual Machine for given {@code taskRequest}
     * @param taskTrackerState state of the task tracker that contains all tasks currently running and assigned
     * @return a value between 0.0 and 1.0, with higher values representing better fit of the task on the virtual
     *         machine
     */
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                                   TaskTrackerState taskTrackerState);
}
