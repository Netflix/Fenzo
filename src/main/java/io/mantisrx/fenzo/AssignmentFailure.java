package io.mantisrx.fenzo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public VMResource getResource() {
        return resource;
    }
    public double getAsking() {
        return asking;
    }
    public double getUsed() {
        return used;
    }
    public double getAvailable() {
        return available;
    }
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.info("Unexpected error writing json: " + e.getMessage());
            return "resource="+resource+", asking="+asking+", used="+used+", available="+available;
        }
    }
}
