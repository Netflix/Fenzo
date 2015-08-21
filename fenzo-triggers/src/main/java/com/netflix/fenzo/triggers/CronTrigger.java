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

import java.util.Date;
import java.util.TimeZone;

import static org.quartz.CronScheduleBuilder.cronSchedule;

/**
 * @warn class description missing
 */
public class CronTrigger<T> extends ScheduledTrigger<T> {
    private String cronExpression;
    private String timeZoneId = "America/Los_Angeles";

    @JsonCreator
    public CronTrigger(@JsonProperty("cronExpression") String cronExpression,
                       @JsonProperty("timeZoneId") String timeZoneId,
                       @JsonProperty("startAt") Date startAt,
                       @JsonProperty("name") String name,
                       @JsonProperty("data") T data,
                       @JsonProperty("dataType") Class<T> dataType,
                       @JsonProperty("action") Class<? extends Action1<T>> action) {
        super(name, data, dataType, action, startAt, null);
        this.cronExpression = cronExpression;
        this.timeZoneId = timeZoneId == null || "".equals(timeZoneId) ? "America/Los_Angeles" : timeZoneId;
        TriggerUtils.validateCronExpression(cronExpression);
    }

    public CronTrigger(String cronExpression,
                       String name,
                       T data,
                       Class<T> dataType,
                       Class<? extends Action1<T>> action) {
        this(cronExpression, "America/Los_Angeles", new Date(), name, data, dataType, action);
    }

    /**
     * @return
     */
    public String getCronExpression() {
        return cronExpression;
    }

    /**
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
    @Override
    @JsonIgnore
    public ScheduleBuilder getScheduleBuilder() {
        return cronSchedule(cronExpression).withMisfireHandlingInstructionDoNothing().inTimeZone(TimeZone.getTimeZone(timeZoneId));
    }

    /**
     * @return
     */
    public String toString() {
        return "CronTrigger (" + getId() + ":" + getName() + ":" + cronExpression + ")";
    }
}
