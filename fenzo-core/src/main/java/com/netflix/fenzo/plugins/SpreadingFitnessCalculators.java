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

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;

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
            VMTaskFitnessCalculator cpuBinPacker = BinPackingFitnessCalculators.cpuBinPacker;
            return 1 - cpuBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
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
            VMTaskFitnessCalculator memoryBinPacker = BinPackingFitnessCalculators.memoryBinPacker;
            return 1 - memoryBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
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
            VMTaskFitnessCalculator networkBinPacker = BinPackingFitnessCalculators.networkBinPacker;
            return 1 - networkBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
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
}
