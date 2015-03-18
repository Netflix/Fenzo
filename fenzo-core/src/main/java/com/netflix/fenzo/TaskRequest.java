package com.netflix.fenzo;

import java.util.List;

public interface TaskRequest {
    public String getId();
    public double getCPUs();
    public double getMemory();
    public double getDisk();
    public int getPorts();
    public List<? extends ConstraintEvaluator> getHardConstraints();
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints();
}
