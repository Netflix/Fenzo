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

import java.util.Iterator;

/**
 * A collection of bin packing fitness calculators.
 */
public class BinPackingFitnessCalculators {

    /**
     * A CPU bin packing fitness calculator. This fitness calculator has the effect of assigning a task to a
     * host that has the least number of available CPUs that are sufficient to fit the task.
     */
    public final static VMTaskFitnessCalculator cpuBinPacker = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "CPUBinPacker";
        }
        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return calculateResourceFitness(taskRequest, targetVM, taskTrackerState,
                    new Func1<TaskRequest, Double>() {
                        @Override
                        public Double call(TaskRequest request) {
                            return request.getCPUs();
                        }
                    },
                    new Func1<VirtualMachineLease, Double>() {
                        @Override
                        public Double call(VirtualMachineLease l) {
                            return l.cpuCores();
                        }
                    });
        }
    };

    /**
     * A memory bin packing fitness calcualtor. This fitness calculator has the effect of assigning a task to a
     * host that has the least amount of available memory that is sufficient to fit the task.
     */
    public final static VMTaskFitnessCalculator memoryBinPacker = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "MemoryBinPacker";
        }
        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return calculateResourceFitness(taskRequest, targetVM, taskTrackerState,
                    new Func1<TaskRequest, Double>() {
                        @Override
                        public Double call(TaskRequest request) {
                            return request.getMemory();
                        }
                    },
                    new Func1<VirtualMachineLease, Double>() {
                        @Override
                        public Double call(VirtualMachineLease l) {
                            return l.memoryMB();
                        }
                    });
        }
    };

    /**
     * A bin packing fitness calculator that achieves both CPU and Memory bin packing with equal weights to
     * both goals.
     */
    public final static VMTaskFitnessCalculator cpuMemBinPacker = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "CPUAndMemoryBinPacker";
        }
        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            double cpuFitness = cpuBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
            double memoryFitness = memoryBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
            return (cpuFitness + memoryFitness) / 2.0;
        }
    };

    /**
     * A network bandwidth bin packing fitness calculator. This fitness calculator has the effect of assigning a
     * task to a host that has the least amount of available network bandwidth that is sufficient for the task.
     */
    public final static VMTaskFitnessCalculator networkBinPacker = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "NetworkBinPacker";
        }
        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return calculateResourceFitness(taskRequest, targetVM, taskTrackerState,
                    new Func1<TaskRequest, Double>() {
                        @Override
                        public Double call(TaskRequest request) {
                            return request.getNetworkMbps();
                        }
                    },
                    new Func1<VirtualMachineLease, Double>() {
                        @Override
                        public Double call(VirtualMachineLease l) {
                            return l.networkMbps();
                        }
                    });
        }
    };

    /**
     * A fitness calculator that achieves CPU, Memory, and network bandwidth bin packing with equal weights to
     * each of the three goals.
     */
    public final static VMTaskFitnessCalculator cpuMemNetworkBinPacker = new VMTaskFitnessCalculator() {
        @Override
        public String getName() {
            return "CPUAndMemoryAndNetworkBinPacker";
        }
        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            double cpuFitness = cpuBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
            double memFitness = memoryBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
            double networkFitness = networkBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
            return (cpuFitness + memFitness + networkFitness)/3.0;
        }
    };

    private static double calculateResourceFitness(TaskRequest request, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState,
                                                   Func1<TaskRequest, Double> taskResourceGetter,
                                                   Func1<VirtualMachineLease, Double> leaseResourceGetter) {
        double totalRes = leaseResourceGetter.call(targetVM.getCurrAvailableResources());
        Iterator<TaskRequest> iterator = targetVM.getRunningTasks().iterator();
        double oldJobsTotal=0.0;
        while(iterator.hasNext())
            oldJobsTotal += taskResourceGetter.call(iterator.next());
        double usedResource = taskResourceGetter.call(request);
        Iterator<TaskAssignmentResult> taskAssignmentResultIterator = targetVM.getTasksCurrentlyAssigned().iterator();
        while(taskAssignmentResultIterator.hasNext())
            usedResource += taskResourceGetter.call(taskAssignmentResultIterator.next().getRequest());
        totalRes += oldJobsTotal;
        usedResource += oldJobsTotal;
        return usedResource / totalRes;
    }

}
