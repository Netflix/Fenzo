/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
