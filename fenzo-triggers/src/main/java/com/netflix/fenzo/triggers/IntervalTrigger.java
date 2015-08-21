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
import org.joda.time.Interval;
import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.spi.MutableTrigger;
import rx.functions.Action1;

public class IntervalTrigger<T> extends ScheduledTrigger<T> {

    private final long repeatInterval;
    private final int repeatCount;

    /**
     * Creates an interval based trigger
     * @param iso8601Interval - See https://en.wikipedia.org/wiki/ISO_8601 for how to specify intervals with start time/end
     *                        time/interval
     * @param repeatCount - repeat count after first trigger. Specify -1 to repeat indefinitely
     * @param name
     * @param data
     * @param dataType
     * @param action
     */
    @JsonCreator
    public IntervalTrigger(@JsonProperty("iso8601Interval") String iso8601Interval,
                           @JsonProperty("repeatCount") int repeatCount,
                           @JsonProperty("name") String name,
                           @JsonProperty("data") T data,
                           @JsonProperty("dataType") Class<T> dataType,
                           @JsonProperty("action") Class<? extends Action1<T>> action) {
        super(name, data, dataType, action, Interval.parse(iso8601Interval).getStart().toDate(), null);
        final Interval jodaInterval = Interval.parse(iso8601Interval);
        this.repeatCount = repeatCount; // -1 is repeat indefinitely
        this.repeatInterval = Interval.parse(iso8601Interval).getEndMillis() - jodaInterval.getStartMillis();
    }

    @Override
    @JsonIgnore
    public ScheduleBuilder getScheduleBuilder() {
        return new ScheduleBuilder<SimpleTrigger>() {
            @Override
            protected MutableTrigger build() {
                return SimpleScheduleBuilder.simpleSchedule()
                        .withRepeatCount(repeatCount)
                        .withIntervalInMilliseconds(repeatInterval)
                        .withMisfireHandlingInstructionNextWithRemainingCount()
                        .build();
            }
        };
    }

    public long getRepeatInterval() {
        return repeatInterval;
    }

    public int getRepeatCount() {
        return repeatCount;
    }
}
