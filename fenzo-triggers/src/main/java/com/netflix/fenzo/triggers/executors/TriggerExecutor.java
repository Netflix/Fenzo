package com.netflix.fenzo.triggers.executors;

import com.netflix.fenzo.triggers.Event;
import com.netflix.fenzo.triggers.Trigger;

/**
 *
 */
public interface TriggerExecutor {
    public Event execute(Trigger trigger) throws Exception;
    public void cancel(Event event) throws Exception;
}
