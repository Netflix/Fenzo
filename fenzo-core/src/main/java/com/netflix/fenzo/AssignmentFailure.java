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
 * @warn class description missing
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
     * @warn method description missing
     *
     * @return
     */
    public VMResource getResource() {
        return resource;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getAsking() {
        return asking;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getUsed() {
        return used;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getAvailable() {
        return available;
    }

    /**
     * @warn method description missing
     *
     * @return
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
