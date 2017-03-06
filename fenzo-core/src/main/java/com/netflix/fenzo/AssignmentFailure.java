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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An assignment failure to indicate that a particular task could not be scheduled because a quantifiable
 * resource was insufficient, along with the amount of the resource that was requested (asked) by the task, the
 * amount already used, and the amount available on the target.
 * <p>
 * When you call {@link TaskScheduler#scheduleOnce(java.util.List, java.util.List) scheduleOnce()} to
 * schedule tasks, that method returns a {@link SchedulingResult}. You can use that object's
 * {@link SchedulingResult#getFailures() getFailures()} method to get a List of
 * {@link TaskAssignmentResult} objects that represent the status of each task you attempted to assign. Each of
 * those objects has a {@link TaskAssignmentResult#getFailures() getFailures()} method with which you can get
 * a list of {@code AssignmentFailure} objects describing any failures of this sort that made the task
 * scheduler unable to assign the task to a host.
 */
public class AssignmentFailure {

    @JsonIgnore
    private static final Logger logger = LoggerFactory.getLogger(AssignmentFailure.class);

    private final VMResource resource;
    private final double asking;
    private final double used;
    private final double available;
    private final String message;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public AssignmentFailure(@JsonProperty("resource") VMResource resource,
                             @JsonProperty("asking") double asking,
                             @JsonProperty("used") double used,
                             @JsonProperty("available") double available,
                             @JsonProperty("message") String message
    ) {
        this.resource = resource;
        this.asking = asking;
        this.used = used;
        this.available = available;
        this.message = message;
    }

    /**
     * Returns which target resource this assignment failure is referring to.
     *
     * @return an enum that indicates which target resource this assignment failure refers to
     */
    public VMResource getResource() {
        return resource;
    }

    /**
     * Returns the quantity of this target resource the task was requesting.
     *
     * @return the quantity of the target resource the task requested
     */
    public double getAsking() {
        return asking;
    }

    /**
     * Returns the quantity of this resource that is already assigned on the target.
     *
     * @return the quantity of the target resource that is already allocated
     */
    public double getUsed() {
        return used;
    }

    /**
     * Returns the quantity of this resource that the target has free to be assigned to a new task or tasks.
     *
     * @return the quantity of the target resource that is available for allocation
     */
    public double getAvailable() {
        return available;
    }

    /**
     * Returns text message associated with this assignment failure.
     *
     * @return a message or null if message not defined
     */
    public String getMessage() {
        return message;
    }

    /**
     * Returns a String representation of this assignment failure, with details about the resource that caused
     * the failure and its current level of allocation and availability on the target.
     *
     * @return a String representation of this assignment failure
     */
    @Override
    public String toString() {
        return "AssignmentFailure{" +
                "resource=" + resource +
                ", asking=" + asking +
                ", used=" + used +
                ", available=" + available +
                ", message=" + message +
                '}';
    }
}
