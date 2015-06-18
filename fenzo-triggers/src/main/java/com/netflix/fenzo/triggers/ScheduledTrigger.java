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
