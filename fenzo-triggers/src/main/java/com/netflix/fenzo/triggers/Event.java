package com.netflix.fenzo.triggers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 */
public class Event {

    private static final Logger logger = LoggerFactory.getLogger(Event.class);

    private final String id;
    private final Trigger trigger;
    private final Job job;
    private Date startTime;
    private Date endTime;
    private Status status;

    public Event(Trigger trigger, Job job) {
        this.id = UUID.randomUUID().toString();
        this.trigger = trigger;
        this.job = job;
    }

    public String getId() {
        return id;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Job getJob() {
        return job;
    }

    public List<String> getExecutionLog() {
        return job.getExecutionLog();
    }

    public Status getStatus() {
        if (this.status == Status.IN_PROGRESS || this.status == Status.FAILED || this.status == Status.CANCELLED) {
            return this.status;
        } else {
            Status jobStatus = job.getStatus();
            return jobStatus != null ? jobStatus : this.status;
        }
    }

    public List<String> getOwners() {
        List<String> owners = job.getOwners();
        return owners == null ? trigger.getOwners() : owners;
    }

    public List<String> getWatchers() {
        List<String> watchers = job.getWatchers();
        return watchers == null ? trigger.getWatchers() : watchers;
    }

    public void notifyForEventStart(List<String> owners, List<String> watchers) {
        try {
            job.notifyForEventStart(owners, watchers);
        } catch (Exception e) {
            logger.error("Exception occurred in notifyForEventStart() method for {}", this, e);
        }
    }

    public void notifyForEventEnd(List<String> owners, List<String> watchers) {
        try {
            job.notifyForEventEnd(owners, watchers);
        } catch (Exception e) {
            logger.error("Exception occurred in notifyForEventEnd() method for {}", this, e);
        }
    }

    public void notifyForError(List<String> owners, List<String> watchers, Throwable throwable) {
        try {
            job.notifyForError(owners, watchers, throwable);
        } catch (Exception e) {
            logger.error("Exception occurred in notifyForError() method for {}", this, e);
        }
    }

    public void notifyForEventCancel(List<String> owners, List<String> watchers) {
        try {
            job.notifyForEventCancel(owners, watchers);
        } catch (Exception e) {
            logger.error("Exception occurred in notifyForEventCancel() method for {}", this, e);
        }
    }

    public void execute(Map<String,Object> params) throws Exception {
        job.execute(params);
    }

    public void cancel() throws Exception {
        job.cancel();
    }

}
