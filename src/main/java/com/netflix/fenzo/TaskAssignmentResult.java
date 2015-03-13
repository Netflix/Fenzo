package com.netflix.fenzo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class TaskAssignmentResult {
    @JsonIgnore
    final private AssignableVirtualMachine avm;
    @JsonIgnore
    final private TaskRequest request;
    final private String taskId;
    final private String hostname;
    final private List<Integer> assignedPorts;
    final private boolean successful;
    final private List<AssignmentFailure> failures;
    final private ConstraintFailure constraintFailure;
    final private double fitness;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    TaskAssignmentResult(@JsonProperty("avm") AssignableVirtualMachine avm,
                         @JsonProperty("request") TaskRequest request,
                         @JsonProperty("successful") boolean successful,
                         @JsonProperty("failures") List<AssignmentFailure> failures,
                         @JsonProperty("constraintFailure") ConstraintFailure constraintFailure,
                         @JsonProperty("fitness") double fitness) {
        this.avm = avm;
        this.request = request;
        this.taskId = request.getId();
        this.hostname = avm==null? "":avm.getHostname();
        this.successful = successful;
        this.failures = failures;
        this.constraintFailure = constraintFailure;
        this.fitness = fitness;
        assignedPorts = new ArrayList<>();
    }

    public String getTaskId() {
        return taskId;
    }
    public String getHostname() {
        return hostname;
    }
    void assignResult() {
        avm.assignResult(this);
    }
    void addPort(int port) {
        assignedPorts.add(port);
    }
    public List<Integer> getAssignedPorts() {
        return assignedPorts;
    }
    @JsonIgnore
    public TaskRequest getRequest() {
        return request;
    }
    public boolean isSuccessful() {
        return successful;
    }
    public List<AssignmentFailure> getFailures() {
        return failures;
    }
    public ConstraintFailure getConstraintFailure() {
        return constraintFailure;
    }
    public double getFitness() {
        return fitness;
    }

}