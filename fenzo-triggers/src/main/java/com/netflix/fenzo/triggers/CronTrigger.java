package com.netflix.fenzo.triggers;

/**
 *
 */
public class CronTrigger extends ScheduledTrigger {
    private String cronExpression;
    private org.quartz.CronTrigger cronTrigger;

    protected CronTrigger(Builder builder) {
        super(builder);
        this.cronExpression = builder.cronExpression;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public org.quartz.CronTrigger getCronTrigger() {
        return cronTrigger;
    }

    public void setCronTrigger(org.quartz.CronTrigger cronTrigger) {
        this.cronTrigger = cronTrigger;
    }

    public static Builder<?> newTrigger() {
        return new Builder();
    }

    public String toString() {
        return "CronTrigger (" + getId() + ":" + getName() + ":" + cronExpression + ")";
    }

    public static class Builder<T extends Builder<T>> extends ScheduledTrigger.Builder<T> {
        private String cronExpression;

        public T withCronExpression(String cronExpression) {
            this.cronExpression = cronExpression;
            return self();
        }

        public CronTrigger build() {
            return new CronTrigger(this);
        }
    }
}
