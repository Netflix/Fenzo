package com.netflix.fenzo.triggers;

import com.netflix.fenzo.triggers.exceptions.EventNotFoundException;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.fenzo.triggers.exceptions.TriggerNotFoundException;
import com.netflix.fenzo.triggers.executors.BlockingThreadPoolJobExecutor;
import com.netflix.fenzo.triggers.executors.SimpleThreadPoolTriggerExecutor;
import com.netflix.fenzo.triggers.executors.TriggerExecutor;
import com.netflix.fenzo.triggers.persistence.EventDao;
import com.netflix.fenzo.triggers.persistence.TriggerDao;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class TriggerService {

    private static final String TRIGGER_KEY = "trigger";
    private static final Logger logger = LoggerFactory.getLogger(TriggerService.class);

    private final Scheduler scheduler;
    private final TriggerDao triggerDao;
    private final EventDao eventDao;
    private final TriggerExecutor triggerExecutor;
    private final int threadPoolSize;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public TriggerService(TriggerDao triggerDao, EventDao eventDao, int threadPoolSize) {
        this.triggerDao = triggerDao;
        this.eventDao = eventDao;
        this.scheduler = Scheduler.getInstance();
        this.threadPoolSize = threadPoolSize;
        this.triggerExecutor = new SimpleThreadPoolTriggerExecutor(new BlockingThreadPoolJobExecutor(eventDao, threadPoolSize), eventDao, threadPoolSize);
    }

    @PostConstruct
    public void initialize() throws SchedulerException {
        if (initialized.compareAndSet(false, true)) {
            try {
                this.scheduler.startScheduler(threadPoolSize);
            } catch (org.quartz.SchedulerException se) {
                throw new SchedulerException("Exception occurred while initializing TriggerService", se);
            }
        }
        for (Iterator<Trigger> iterator = getTriggers().iterator(); iterator.hasNext();) {
            Trigger trigger = iterator.next();
            if (!trigger.isDisabled() && trigger instanceof ScheduledTrigger) {
                scheduleTrigger((ScheduledTrigger) trigger);
            }
        }
    }

    public Trigger getTrigger(String triggerId) {
        return triggerDao.getTrigger(triggerId);
    }

    public void registerTrigger(String triggerGroup, Trigger trigger) throws SchedulerException {
        triggerDao.createTrigger(triggerGroup, trigger);
        if (trigger instanceof ScheduledTrigger) {
            scheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    public void enableTrigger(String triggerGroup, String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            trigger.setDisabled(false);
            triggerDao.updateTrigger(triggerGroup, trigger);
            if (trigger instanceof ScheduledTrigger) {
                scheduleTrigger((ScheduledTrigger) trigger);
            }
        } else {
            throw new TriggerNotFoundException("No trigger found for trigger id: " + triggerId);
        }
    }

    public void disableTrigger(String triggerGroup, String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            trigger.setDisabled(true);
            triggerDao.updateTrigger(triggerGroup, trigger);
            if (trigger instanceof ScheduledTrigger) {
                unscheduleTrigger((ScheduledTrigger) trigger);
            }
        } else {
            throw new TriggerNotFoundException("No trigger found for trigger id: " + triggerId);
        }
    }

    public void deleteTrigger(String triggerGroup, String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            triggerDao.deleteTrigger(triggerGroup, trigger);
            if (trigger instanceof ScheduledTrigger) {
                scheduleTrigger((ScheduledTrigger) trigger);
            }
        } else {
            throw new TriggerNotFoundException("No trigger found for trigger id: " + triggerId);
        }
    }

    protected void scheduleTrigger(ScheduledTrigger scheduledTrigger) throws SchedulerException {
        if (!initialized.get()) throw new SchedulerException("Trigger service is not initialized. initialize() must be called before calling scheduleTrigger() method");
        Map jobDataMap = new HashMap();
        jobDataMap.put(TRIGGER_KEY, scheduledTrigger);
        try {
            CronTrigger cronTrigger = (CronTrigger) scheduledTrigger;
            org.quartz.CronTrigger quartzCronTrigger = scheduler.scheduleQuartzJob(ScheduledTriggerJob.class,
                cronTrigger.getId(),
                Scheduler.DEFAULT_GROUP,
                jobDataMap,
                cronTrigger.getCronExpression());
            cronTrigger.setCronTrigger(quartzCronTrigger);
            logger.info("Successfully scheduled {}", scheduledTrigger);
        } catch (org.quartz.SchedulerException e) {
            throw new SchedulerException("Exception occurred while scheduling trigger: " + scheduledTrigger, e);
        }
    }

    protected class ScheduledTriggerJob implements org.quartz.Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Trigger trigger = (Trigger) context.getMergedJobDataMap().get(TRIGGER_KEY);
            try {
                TriggerService.this.execute(trigger);
            } catch (Exception e) {
                throw new JobExecutionException(e);
            }
        }
    }

    public void unscheduleTrigger(ScheduledTrigger scheduledTrigger) throws SchedulerException {
        if (!initialized.get()) throw new SchedulerException("Trigger service is not initialized. initialize() must be called before calling unscheduleTrigger() method");
        try {
            scheduler.unscheduleQuartzJob(scheduledTrigger.getId(), Scheduler.DEFAULT_GROUP);
            logger.info("Successfully unscheduled {}", scheduledTrigger);
        } catch (org.quartz.SchedulerException e) {
            throw new SchedulerException("Exception occurred while unscheduling trigger: " + scheduledTrigger, e);
        }
    }

    public List<Trigger> getTriggers(String triggerGroup) {
        return triggerDao.getTriggers(triggerGroup);
    }

    public List<Trigger> getTriggers() {
        return triggerDao.getTriggers();
    }

    public Event getEvent(String eventId) {
        return eventDao.getEvent(eventId);
    }

    public List<Event> getEvents(String triggerId) {
        return eventDao.getEvents(triggerId);
    }

    public List<Event> getEvents(String triggerId, int count) {
        return eventDao.getEvents(triggerId, count);
    }

    public Event execute(Trigger trigger) throws Exception {
        try {
            return triggerExecutor.execute(trigger);
        } catch (Exception e) {
            throw new Exception(String.format("Exception occurred while executing trigger '%s'", trigger), e);
        }
    }

    public void cancelEvent(String eventId) throws Exception {
        Event event = eventDao.getEvent(eventId);
        if (event != null) {
            triggerExecutor.cancel(event);
        } else {
            throw new EventNotFoundException("No event found with event id: " + eventId);
        }
    }

}
