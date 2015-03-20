package com.netflix.fenzo.triggers;

import org.quartz.ScheduleBuilder;
import rx.functions.Action1;

import static org.quartz.CronScheduleBuilder.cronSchedule;

/**
 *
 */
public class CronTrigger<T> extends ScheduledTrigger<T> {
    private String cronExpression;
    private org.quartz.Trigger quartzTrigger;

    protected CronTrigger(String cronExpression, String name, T data, Action1<T> action) {
        super(name, data, action);
        this.cronExpression = cronExpression;
    }

    protected org.quartz.Trigger getQuartzTrigger() {
        return quartzTrigger;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public ScheduleBuilder getScheduleBuilder() {
        return cronSchedule(cronExpression);
    }

    public void setQuartzTrigger(org.quartz.Trigger quartzTrigger) {
        this.quartzTrigger = quartzTrigger;
    }

    public String toString() {
        return "CronTrigger (" + getId() + ":" + getName() + ":" + cronExpression + ")";
    }
}
