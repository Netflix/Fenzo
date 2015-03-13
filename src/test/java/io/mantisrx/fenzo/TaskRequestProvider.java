package io.mantisrx.fenzo;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class TaskRequestProvider {

    private static final AtomicInteger id = new AtomicInteger();

    static TaskRequest getTaskRequest(final double cpus, final double memory, final int ports) {
        return getTaskRequest(cpus, memory, ports, null, null);
    }
    static TaskRequest getTaskRequest(final double cpus, final double memory, final int ports,
                                      final List<? extends ConstraintEvaluator> hardConstraints,
                                      final List<? extends VMTaskFitnessCalculator> softConstraints) {
        final String taskId = ""+id.incrementAndGet();
        return new TaskRequest() {
            @Override
            public String getId() {
                return taskId;
            }
            @Override
            public double getCPUs() {
                return cpus;
            }
            @Override
            public double getMemory() {
                return memory;
            }
            @Override
            public double getDisk() {
                return 0;
            }
            @Override
            public int getPorts() {
                return ports;
            }
            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return hardConstraints;
            }
            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return softConstraints;
            }
        };
    }
}
