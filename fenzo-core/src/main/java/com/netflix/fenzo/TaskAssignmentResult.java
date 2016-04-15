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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the results of attempting to assign a task to a host.
 */
public class TaskAssignmentResult {
    @JsonIgnore
    final private AssignableVirtualMachine avm;
    @JsonIgnore
    final private TaskRequest request;
    final private String taskId;
    final private String hostname;
    final private List<Integer> assignedPorts;
    final private List<PreferentialNamedConsumableResourceSet.ConsumeResult> rSets;
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
        rSets = new ArrayList<>();
    }

    /**
     * Returns the string identifier of the task request for the task whose assignment result this is.
     *
     * @return the identifier of the task request
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * Returns the name of the host machine to which this task was attempted to be assigned.
     *
     * @return the hostname
     */
    public String getHostname() {
        return hostname;
    }

    void assignResult() {
        avm.assignResult(this);
    }

    void addPort(int port) {
        assignedPorts.add(port);
    }

    void addResourceSet(PreferentialNamedConsumableResourceSet.ConsumeResult rSet) {
        rSets.add(rSet);
    }

    /**
     * Returns a list of port numbers corresponding to the ports the task was assigned on the host.
     *
     * @return a list of port numbers
     */
    public List<Integer> getAssignedPorts() {
        return assignedPorts;
    }

    public List<PreferentialNamedConsumableResourceSet.ConsumeResult> getrSets() {
        return rSets;
    }

    /**
     * Returns the {@link TaskRequest} corresponding to the task whose assignment result this is.
     *
     * @return the {@code TaskRequest} for this task
     */
    @JsonIgnore
    public TaskRequest getRequest() {
        return request;
    }

    /**
     * Indicates whether the assignment of this task to the host succeeded.
     *
     * @return {@code true} if this assignment succeeded, {@code false} otherwise
     */
    public boolean isSuccessful() {
        return successful;
    }

    /**
     * Get a list of {@link AssignmentFailure}s corresponding to the reasons why the assignment of this task to
     * the host did not succeed because of insufficient resources.
     *
     * @return a list of reasons why the task could not be assigned to the host because of insufficient
     *         resources
     */
    public List<AssignmentFailure> getFailures() {
        return failures;
    }

    /**
     * Get the {@link ConstraintFailure} corresponding to the task constraint that the host failed to meet.
     *
     * @return information about the constraint that the host failed to satisfy
     */
    public ConstraintFailure getConstraintFailure() {
        return constraintFailure;
    }

    /**
     * Get the result of the fitness calculation applied to this host for this task.
     *
     * @return a number between 0.0 (indicating that the host is completely unfit for this task) to 1.0
     *         (indicating that the host is a perfect fit for this task)
     */
    public double getFitness() {
        return fitness;
    }

    @Override
    public String toString() {
        return "TaskAssignmentResult{" +
                "host=" + avm.getHostname() +
                ", request=" + request +
                ", taskId='" + taskId + '\'' +
                ", hostname='" + hostname + '\'' +
                ", assignedPorts=" + assignedPorts +
                ", successful=" + successful +
                ", failures=" + failures +
                ", constraintFailure=" + constraintFailure +
                ", fitness=" + fitness +
                '}';
    }
}
