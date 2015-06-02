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
