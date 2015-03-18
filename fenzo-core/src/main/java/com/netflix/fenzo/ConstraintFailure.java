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
    public String getName() {
        return name;
    }
    public String getReason() {
        return reason;
    }
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.info("Unexpected error writing json: " + e.getMessage());
            return "Constraint="+name+", reason="+reason;
        }
    }
}
