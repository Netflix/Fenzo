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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;
import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.spi.MutableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

public class IntervalTrigger<T> extends ScheduledTrigger<T> {
    private static final Logger logger = LoggerFactory.getLogger(IntervalTrigger.class);
    final private long repeatInterval;
    final private int repeatCount;

    public IntervalTrigger(
            @JsonProperty("iso8601Interval") String interval,
            @JsonProperty("repeatCount") int repeatCount,
            @JsonProperty("name") String name,
            @JsonProperty("data") T data,
            @JsonProperty("dataType") Class<T> dataType,
            @JsonProperty("action") Class<? extends Action1<T>> action) {
        super(name, data, dataType, action, Interval.parse(interval).getStart().toDate(), null);

        final Interval jodaInterval = Interval.parse(interval);
        this.repeatCount = repeatCount; // -1 = indefinitely repeat
        repeatInterval = Interval.parse(interval).getEndMillis() - jodaInterval.getStartMillis();
    }

    @Override
    public ScheduleBuilder getScheduleBuilder() {
        return new ScheduleBuilder<SimpleTrigger>() {
            @Override
            protected MutableTrigger build() {
                return SimpleScheduleBuilder.simpleSchedule()
                        .withRepeatCount(repeatCount)
                        .withIntervalInMilliseconds(repeatInterval)
                        .build();
            }
        };
    }

    @Override
    public void setQuartzTrigger(Trigger quartzTrigger) {
        logger.info("Setting QuartzTrigger " + quartzTrigger);
    }
}
