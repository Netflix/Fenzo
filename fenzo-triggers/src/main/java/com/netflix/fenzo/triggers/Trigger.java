package com.netflix.fenzo.triggers;

import com.netflix.fenzo.triggers.plugins.JobDecorator;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Base class for all types of triggers
 */
public class Trigger {

    private final String id;
    private final Date createdDate;
    private String name;
    private Class<Job> jobClass;
    private Map<String, Object> parameters;
    private List<String> owners;
    private List<String> watchers;
    private int maxEventsToKeep;
    private int maxDaysToKeepEventsFor;
    private boolean notificationsEnabled;
    private boolean disabled;
    private boolean concurrentEventsEnabled;
    private JobDecorator jobDecorator;
    private long executionTimeoutInSeconds;

    protected Trigger(Builder builder) {
        this.id = UUID.randomUUID().toString();
        this.createdDate = new Date();
        this.name = builder.name;
        this.jobClass = builder.jobClass;
        this.parameters = builder.parameters;
        this.owners = builder.owners;
        this.watchers = builder.watchers;
        this.maxDaysToKeepEventsFor = builder.maxDaysToKeepEventsFor;
        this.maxEventsToKeep = builder.maxEventsToKeep;
        this.notificationsEnabled = builder.notificationsEnabled;
        this.concurrentEventsEnabled = builder.concurrentEventsEnabled;
        this.jobDecorator = builder.jobDecorator;
        this.executionTimeoutInSeconds = builder.executionTimeoutInSeconds;
    }

    public String getId() {
        return id;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class<Job> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<Job> jobClass) {
        this.jobClass = jobClass;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }

    public List<String> getWatchers() {
        return watchers;
    }

    public void setWatchers(List<String> watchers) {
        this.watchers = watchers;
    }

    public int getMaxEventsToKeep() {
        return maxEventsToKeep;
    }

    public void setMaxEventsToKeep(int maxEventsToKeep) {
        this.maxEventsToKeep = maxEventsToKeep;
    }

    public int getMaxDaysToKeepEventsFor() {
        return maxDaysToKeepEventsFor;
    }

    public void setMaxDaysToKeepEventsFor(int maxDaysToKeepEventsFor) {
        this.maxDaysToKeepEventsFor = maxDaysToKeepEventsFor;
    }

    public boolean isNotificationsEnabled() {
        return notificationsEnabled;
    }

    public void setNotificationsEnabled(boolean notificationsEnabled) {
        this.notificationsEnabled = notificationsEnabled;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public boolean isConcurrentEventsEnabled() {
        return concurrentEventsEnabled;
    }

    public void setConcurrentEventsEnabled(boolean concurrentEventsEnabled) {
        this.concurrentEventsEnabled = concurrentEventsEnabled;
    }

    public JobDecorator getJobDecorator() {
        return jobDecorator;
    }

    public void setJobDecorator(JobDecorator jobDecorator) {
        this.jobDecorator = jobDecorator;
    }

    public long getExecutionTimeoutInSeconds() {
        return executionTimeoutInSeconds;
    }

    public void setExecutionTimeoutInSeconds(long executionTimeoutInSeconds) {
        this.executionTimeoutInSeconds = executionTimeoutInSeconds;
    }

    public String toString() {
        return "Trigger (" + id + ":" + name + ")";
    }

    /**
     * Trigger builder
     */
    public static class Builder<T extends Builder<T>> {
        private String name;
        private Class<Job> jobClass;
        private Map<String,String> parameters;
        private List<String> owners;
        private List<String> watchers;
        private int maxEventsToKeep = 20;
        private int maxDaysToKeepEventsFor = 1;
        private boolean notificationsEnabled;
        private boolean concurrentEventsEnabled = false;
        private JobDecorator jobDecorator;
        private long executionTimeoutInSeconds;

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        public T withName(String name) {
            this.name = name;
            return self();
        }

        public T withOwners(List<String> owners) {
            this.owners = owners;
            return self();
        }

        public T withJobClass(Class<Job> jobClass) {
            this.jobClass = jobClass;
            return self();
        }

        public T withParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return self();
        }

        public T withWatchers(List<String> watchers) {
            this.watchers = watchers;
            return self();
        }

        public T withMaxDaysToKeepEventsFor(int maxDaysToKeepEventsFor) {
            this.maxDaysToKeepEventsFor = maxDaysToKeepEventsFor;
            return self();
        }

        public T withMaxEventsToKeep(int maxEventsToKeep) {
            this.maxEventsToKeep = maxEventsToKeep;
            return self();
        }

        public T withNotificationsEnabled(boolean notificationsEnabled) {
            this.notificationsEnabled = notificationsEnabled;
            return self();
        }

        public T withConcurrentEventsEnabled(boolean concurrentEventsEnabled) {
            this.concurrentEventsEnabled = concurrentEventsEnabled;
            return self();
        }

        public T withJobDecorator(JobDecorator jobDecorator) {
            this.jobDecorator = jobDecorator;
            return self();
        }

        public T withEventExecutionTimeout(long executionTimeoutInSeconds) {
            this.executionTimeoutInSeconds = executionTimeoutInSeconds;
            return self();
        }

        public Trigger build() {
            return new Trigger(this);
        }
    }

    public static Builder<?> newTrigger() {
        return new Builder();
    }
}
