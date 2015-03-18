package com.netflix.fenzo.triggers;

import rx.functions.Action1;

/**
 *
 */
public class CronTrigger<T> extends ScheduledTrigger<T> {
    private String cronExpression;
    private org.quartz.CronTrigger cronTrigger;

    protected CronTrigger(String cronExpression, String name, T data, Action1<T> action) {
        super(name, data, action);
        this.cronExpression = cronExpression;
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

    public String toString() {
        return "CronTrigger (" + getId() + ":" + getName() + ":" + cronExpression + ")";
    }
}
