package com.netflix.fenzo.triggers;

/**
 *
 */
public class ApplicationTrigger extends Trigger {

    private String applicationName;

    protected ApplicationTrigger(Builder builder) {
        super(builder);
        this.applicationName = builder.applicationName;
    }

    public static Builder<?> newTrigger() {
        return new Builder();
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String toString() {
        return "ApplicationTrigger (" + applicationName + ":" + getId() + ":" + getName() + ")";
    }

    public static class Builder<T extends Builder<T>> extends Trigger.Builder<T> {
        private String applicationName;

        public T withApplicationName(String applicationName) {
            this.applicationName = applicationName;
            return self();
        }

        public ApplicationTrigger build() {
            return new ApplicationTrigger(this);
        }
    }
}
