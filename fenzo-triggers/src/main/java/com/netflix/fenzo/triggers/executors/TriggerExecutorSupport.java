package com.netflix.fenzo.triggers.executors;

import com.netflix.fenzo.triggers.Status;
import com.netflix.fenzo.triggers.Job;
import com.netflix.fenzo.triggers.persistence.EventDao;
import com.netflix.fenzo.triggers.Event;
import com.netflix.fenzo.triggers.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public abstract class TriggerExecutorSupport implements TriggerExecutor {

    private final static Logger logger = LoggerFactory.getLogger(TriggerExecutorSupport.class);
    protected final EventDao eventDao;

    protected TriggerExecutorSupport(EventDao eventDao) {
        this.eventDao = eventDao;
    }

    protected boolean isTriggerExecuting(Trigger trigger) {
        List<Event> eventsForTrigger = eventDao.getEvents(trigger.getId());
        for (Event existingEvent : eventsForTrigger) {
            if (existingEvent.getStatus() == Status.IN_PROGRESS) {
                logger.info("An event '{}' for trigger '{}' is already running. Skipping...", existingEvent, trigger);
                return true;
            }
        }
        return false;
    }

    protected Job createJobInstance(Trigger trigger) throws Exception {
        try {
            Job job = trigger.getJobClass().newInstance();
            if (trigger.getJobDecorator() != null) {
                job = trigger.getJobDecorator().decorate(job, trigger.getParameters());
            }
            return job;
        } catch (Exception e) {
            throw new Exception(
                String.format("Exception occurred while creating job instance '%s' for trigger '%s'", trigger.getJobClass(), trigger), e);
        }
    }

    protected Event createEvent(Trigger trigger, Job job) throws Exception {
        try {
            Event event = new Event(trigger, job);
            eventDao.createEvent(trigger.getId(), event);
            return event;
        } catch (Exception e) {
            throw new Exception(String.format("Exception occurred while creating event for trigger '%s'", trigger), e);
        }
    }

}
