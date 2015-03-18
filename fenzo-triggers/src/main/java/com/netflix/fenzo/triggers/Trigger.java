package com.netflix.fenzo.triggers;

import rx.functions.Action1;

import java.util.Date;
import java.util.UUID;

/**
 * Base class for all types of triggers
 */
public class Trigger {

    private final String id;
    private final Date createdDate;
    private final String inputId;
    private final Action1<String> action;
    private final String name;
    private boolean disabled;

    protected Trigger(Builder builder) {
        this.id = UUID.randomUUID().toString();
        this.createdDate = new Date();
        this.name = builder.name;
        this.inputId = builder.inputId;
        this.action = builder.action;
    }

    public String getId() {
        return id;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public String getInputId() {
        return inputId;
    }

    public Action1<String> getAction() {
        return action;
    }

    public String getName() {
        return name;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public String toString() {
        return "Trigger (" + id + ":" + name + ")";
    }

    /**
     * Trigger builder
     */
    public static class Builder<T extends Builder<T>> {
        private String name;
        private String inputId;
        private Action1<String> action;

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        public T withName(String name) {
            this.name = name;
            return self();
        }

        public T withInputId(String inputId) {
            this.inputId = inputId;
            return self();
        }

        public T withAction1(Action1<String> action) {
            this.action = action;
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
