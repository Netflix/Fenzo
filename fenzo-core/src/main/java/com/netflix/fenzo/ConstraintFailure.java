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
 * An object that encapsulates the case of a target failing to satisfy a constraint mandated by a task and
 * implemented by a {@link ConstraintEvaluator}.
 */
public class ConstraintFailure {
    @JsonIgnore
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @JsonIgnore
    private static final Logger logger = LoggerFactory.getLogger(ConstraintFailure.class);
    private final String name;
    private final String reason;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public ConstraintFailure(@JsonProperty("name") String name, @JsonProperty("reason") String reason) {
        this.name = name;
        this.reason = reason;
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Returns the name of the constraint that was violated by the target.
     *
     * @return the name of the constraint
     */
    public String getName() {
        return name;
    }

    /**
     * Returns a description of how the constraint was violated by the target.
     *
     * @return a description of how the constraint failed
     */
    public String getReason() {
        return reason;
    }

    /**
     * Returns a textual description of the constraint failure that combines the name of the constraint and
     * the reason why it was violated by the target.
     *
     * @return a String representation of this {@code ConstraintFailure}
     */
    @Override
    public String toString() {
        return "ConstraintFailure{" +
                "name='" + name + '\'' +
                ", reason='" + reason + '\'' +
                '}';
    }
}
