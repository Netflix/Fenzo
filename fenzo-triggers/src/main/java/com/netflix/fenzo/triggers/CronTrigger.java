package com.netflix.fenzo.triggers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.quartz.ScheduleBuilder;
import rx.functions.Action1;

import static org.quartz.CronScheduleBuilder.cronSchedule;

/**
 *
 */
public class CronTrigger<T> extends ScheduledTrigger<T> {
    private String cronExpression;
    @JsonIgnore
    private org.quartz.Trigger quartzTrigger;

    @JsonCreator
    public CronTrigger(@JsonProperty("cronExpression") String cronExpression,
                       @JsonProperty("name") String name,
                       @JsonProperty("data") T data,
                       @JsonProperty("dataType") Class<T> dataType,
                       @JsonProperty("action") Class<? extends Action1<T>> action) {
        super(name, data, dataType, action);
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

    @JsonIgnore
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
