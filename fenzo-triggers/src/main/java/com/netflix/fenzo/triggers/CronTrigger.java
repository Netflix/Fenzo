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
 * @warn class description missing
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
        TriggerUtils.validateCronExpression(cronExpression);
    }

    protected org.quartz.Trigger getQuartzTrigger() {
        return quartzTrigger;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public String getCronExpression() {
        return cronExpression;
    }

    /**
     * @warn method description missing
     * @warn parameter cronExpression description missing
     *
     * @param cronExpression
     */
    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    @JsonIgnore
    public ScheduleBuilder getScheduleBuilder() {
        return cronSchedule(cronExpression);
    }

    /**
     * @warn method description missing
     * @warn parameter quartzTrigger description missing
     *
     * @param quartzTrigger
     */
    public void setQuartzTrigger(org.quartz.Trigger quartzTrigger) {
        this.quartzTrigger = quartzTrigger;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public String toString() {
        return "CronTrigger (" + getId() + ":" + getName() + ":" + cronExpression + ")";
    }
}
