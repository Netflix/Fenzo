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
 * Interface representing a task fitness calculator. A task may fit on multiple hosts. Use this fitness calculator to
 * determine how well a task fits on a host.
 */
public interface VMTaskFitnessCalculator {
    /**
     * Get the name of this fitness calculator.
     *
     * @return Name of the fitness calculator.
     */
    public String getName();

    /**
     * This is used in the {@link TaskScheduler} during a scheduling trial, after a task's resource requirements are
     * met by a host. Use this to calculate how well the task fits on the host.
     *
     * @param taskRequest      the task whose resource requirements can be met by the Virtual Machine
     * @param targetVM         the prospective target host (VM) for given {@code taskRequest}
     * @param taskTrackerState state of the task tracker that contains all tasks currently running and assigned
     * @return a value between 0.0 and 1.0, with higher values representing better fit of the task on the virtual
     *         machine
     */
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                                   TaskTrackerState taskTrackerState);
}
