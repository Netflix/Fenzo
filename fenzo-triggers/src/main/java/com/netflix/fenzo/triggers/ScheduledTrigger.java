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

/**
 * Placeholder super class for all the triggers that can be scheduled.
 */
public abstract class ScheduledTrigger<T> extends Trigger<T> {
    private final Date startAt;
    private final Date endAt;
    @JsonIgnore
    private org.quartz.Trigger quartzTrigger;

    @JsonCreator
    public ScheduledTrigger(
            @JsonProperty("name") String name,
            @JsonProperty("data") T data,
            @JsonProperty("dataType") Class<T> dataType,
            @JsonProperty("action") Class<? extends Action1<T>> action,
            @JsonProperty("startAt") Date startAt,
            @JsonProperty("endAt") Date endAt) {
        super(name, data, dataType, action);
        this.startAt = startAt;
        this.endAt = endAt;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public abstract ScheduleBuilder getScheduleBuilder();

    /**
     * @warn method description missing
     * @warn param quartzTrigger description missing
     *
     * @param quartzTrigger
     */
    void setQuartzTrigger(org.quartz.Trigger quartzTrigger) {
        this.quartzTrigger = quartzTrigger;
    }

    org.quartz.Trigger getQuartzTrigger() {
        return quartzTrigger;
    }

    public Date getStartAt() {
        return startAt;
    }

    public Date getEndAt() {
        return endAt;
    }

    public Date getNextFireTime() {
        return quartzTrigger != null ? quartzTrigger.getNextFireTime() : null;
    }

    public Date getPreviousFireTime() {
        return quartzTrigger != null ? quartzTrigger.getPreviousFireTime() : null;
    }
}
