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

package com.netflix.fenzo.plugins;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Func1;

/**
 * A collection of spreading fitness calculators.
 */
public class SpreadingFitnessCalculators {

    /**
     * A CPU spreading fitness calculator. This fitness calculator has the effect of assigning a task to a
     * host that has the most CPUs available.
     */
    public final static VMTaskFitnessCalculator cpuSpreader = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "CpuSpreader";
        }

        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return calculateResourceFitness(taskRequest, targetVM, taskTrackerState, TaskRequest::getCPUs, VirtualMachineLease::cpuCores);
        }
    };

    /**
     * A memory bin packing fitness calculator. This fitness calculator has the effect of assigning a task to a
     * host that has the most memory available.
     */
    public final static VMTaskFitnessCalculator memorySpreader = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "MemorySpreader";
        }

        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return calculateResourceFitness(taskRequest, targetVM, taskTrackerState, TaskRequest::getMemory, VirtualMachineLease::memoryMB);
        }
    };

    /**
     * A bin packing fitness calculator that achieves both CPU and Memory spreading with equal weights to
     * both goals.
     */
    public final static VMTaskFitnessCalculator cpuMemSpreader = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "CpuAndMemorySpreader";
        }

        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            double cpuFitness = cpuSpreader.calculateFitness(taskRequest, targetVM, taskTrackerState);
            double memoryFitness = memorySpreader.calculateFitness(taskRequest, targetVM, taskTrackerState);
            return (cpuFitness + memoryFitness) / 2.0;
        }
    };

    /**
     * A network bandwidth spreading fitness calculator. This fitness calculator has the effect of assigning a
     * task to a host that has the most amount of available network bandwidth.
     */
    public final static VMTaskFitnessCalculator networkSpreader = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "NetworkSpreader";
        }

        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return calculateResourceFitness(taskRequest, targetVM, taskTrackerState, TaskRequest::getNetworkMbps, VirtualMachineLease::networkMbps);
        }
    };

    /**
     * A fitness calculator that achieves CPU, Memory, and network bandwidth spreading with equal weights to
     * each of the three goals.
     */
    public final static VMTaskFitnessCalculator cpuMemNetworkSpreader = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "CPUAndMemoryAndNetworkBinPacker";
        }

        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            double cpuFitness = cpuSpreader.calculateFitness(taskRequest, targetVM, taskTrackerState);
            double memFitness = memorySpreader.calculateFitness(taskRequest, targetVM, taskTrackerState);
            double networkFitness = networkSpreader.calculateFitness(taskRequest, targetVM, taskTrackerState);
            return (cpuFitness + memFitness + networkFitness) / 3.0;
        }
    };

    private static double calculateResourceFitness(TaskRequest request, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState,
                                                   Func1<TaskRequest, Double> taskResourceGetter,
                                                   Func1<VirtualMachineLease, Double> leaseResourceGetter) {

        // The resources currently being used by running tasks
        double usedResources = 0.0;
        for (TaskRequest taskRequest : targetVM.getRunningTasks()) {
            usedResources += taskResourceGetter.call(taskRequest);
        }

        // The total amount of resources on a VM
        double totalResources = leaseResourceGetter.call(targetVM.getCurrAvailableResources()) + usedResources;

        // The amount of resources that have been requested in this scheduling run
        double requestedResources = 0.0;
        for (TaskAssignmentResult taskAssignmentResult : targetVM.getTasksCurrentlyAssigned()) {
            requestedResources += taskResourceGetter.call(taskAssignmentResult.getRequest());
        }

        // The amount of resources that are available to use
        double availableResources = totalResources - (usedResources + requestedResources);
        return availableResources / totalResources;
    }
}
