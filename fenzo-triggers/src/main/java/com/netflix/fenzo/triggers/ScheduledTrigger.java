package com.netflix.fenzo.triggers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.quartz.ScheduleBuilder;
import rx.functions.Action1;

/**
 * Placeholder super class for all the triggers that can be scheduled.
 *
 */
public abstract class ScheduledTrigger<T> extends Trigger<T> {

    @JsonCreator
    public ScheduledTrigger(@JsonProperty("name") String name,
                            @JsonProperty("data") T data,
                            @JsonProperty("dataType") Class<T> dataType,
                            @JsonProperty("action") Class<? extends Action1<T>> action) {
        super(name, data, dataType, action);
    }

    public abstract ScheduleBuilder getScheduleBuilder();

    public abstract void setQuartzTrigger(org.quartz.Trigger quartzTrigger);
}
