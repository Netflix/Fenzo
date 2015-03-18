package com.netflix.fenzo.triggers.executors;

import com.netflix.fenzo.triggers.Job;
import com.netflix.fenzo.triggers.Event;

/**
 *
 */
public interface JobExecutor {
    public void execute(Job job, Event event);
    public void cancel(Job job, Event event);
}
