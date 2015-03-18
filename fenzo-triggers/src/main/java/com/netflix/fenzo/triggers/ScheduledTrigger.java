package com.netflix.fenzo.triggers;

/**
 * Placeholder super class for all the triggers that can be scheduled.
 *
 */
public abstract class ScheduledTrigger extends Trigger {

    protected ScheduledTrigger(Builder builder) {
        super(builder);
    }

    public static class Builder<T extends Builder<T>> extends Trigger.Builder<T> {}

}
