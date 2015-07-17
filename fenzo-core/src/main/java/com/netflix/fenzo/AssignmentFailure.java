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
 * Indicates that a quantifiable resource required in a certain quantity by a task was unavailable in that
 * quantity by a target.
 */
public class AssignmentFailure {
    @JsonIgnore
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @JsonIgnore
    private static final Logger logger = LoggerFactory.getLogger(AssignmentFailure.class);
    private VMResource resource;
    private double asking;
    private double used;
    private double available;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public AssignmentFailure(@JsonProperty("resource") VMResource resource,
                             @JsonProperty("asking") double asking,
                             @JsonProperty("used") double used,
                             @JsonProperty("available") double available) {
        this.resource = resource;
        this.asking = asking;
        this.used = used;
        this.available = available;
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
     * Returns a String representation of this assignment failure, with details about the resource that caused
     * the failure and its current level of allocation and availability on the target.
     *
     * @return a String representation of this assignment failure
     */
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.info("Unexpected error writing json: " + e.getMessage());
            return "resource="+resource+", asking="+asking+", used="+used+", available="+available;
        }
    }
}
