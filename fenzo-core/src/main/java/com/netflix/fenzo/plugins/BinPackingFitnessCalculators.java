package com.netflix.fenzo.plugins;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import rx.functions.Func1;

import java.util.Iterator;

public class BinPackingFitnessCalculators {

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
