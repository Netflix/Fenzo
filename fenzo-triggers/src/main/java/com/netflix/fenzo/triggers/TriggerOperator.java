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

import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.fenzo.triggers.exceptions.TriggerNotFoundException;
import com.netflix.fenzo.triggers.persistence.InMemoryTriggerDao;
import com.netflix.fenzo.triggers.persistence.TriggerDao;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

/**
 *
 */
public class TriggerOperator {

    public static final String TRIGGER_KEY = "trigger";
    public static final String TRIGGER_OPERATOR_KEY = "triggerOperator";

    private static final Logger logger = LoggerFactory.getLogger(TriggerOperator.class);

    private final Scheduler scheduler;
    private final TriggerDao triggerDao;
    private final int threadPoolSize;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Constructor
     * @param triggerDao - dao implementation for {@code Trigger}
     * @param threadPoolSize - the thread pool size for the scheduler
     */
    public TriggerOperator(TriggerDao triggerDao, int threadPoolSize) {
        this.triggerDao = triggerDao;
        this.scheduler = Scheduler.getInstance();
        this.threadPoolSize = threadPoolSize;
    }

    /**
     * Users of this class must call {@code initialize()} before using this class
     * @throws SchedulerException
     */
    @PostConstruct
    public void initialize() throws SchedulerException {
        if (initialized.compareAndSet(false, true)) {
            try {
                this.scheduler.startScheduler(threadPoolSize);
            } catch (org.quartz.SchedulerException se) {
                throw new SchedulerException("Exception occurred while initializing TriggerOperator", se);
            }
            for (Iterator<Trigger> iterator = getTriggers().iterator(); iterator.hasNext();) {
                Trigger trigger = iterator.next();
                if (!trigger.isDisabled() && trigger instanceof ScheduledTrigger) {
                    scheduleTrigger((ScheduledTrigger) trigger);
                }
            }
        }
    }

    @PreDestroy
    public void destroy() throws SchedulerException {
        if (initialized.get() && this.scheduler != null) {
            try {
                this.scheduler.stopScheduler(true);
            } catch (org.quartz.SchedulerException se) {
                throw new SchedulerException("Exception occurred while destroying TriggerOperator", se);
            }
        }
    }

    /**
     * Returns a default instance of {@code TriggerOperator} with sensible default values.
     * Uses an in-memory implementation of Dao.
     * @return
     */
    public static TriggerOperator getInstance() {
        return new TriggerOperator(new InMemoryTriggerDao(), 20);
    }

    /**
     *
     * @param threadPoolSize
     * @return
     */
    public static TriggerOperator getInstance(int threadPoolSize) {
        return new TriggerOperator(new InMemoryTriggerDao(), threadPoolSize);
    }

    /**
     *
     * @param triggerDao
     * @param threadPoolSize
     * @return
     */
    public static TriggerOperator getInstance(TriggerDao triggerDao, int threadPoolSize) {
        return new TriggerOperator(triggerDao, threadPoolSize);
    }

    /**
     * Returns the {@code Trigger} based on the unique trigger id
     * @param triggerId
     * @return
     */
    public Trigger getTrigger(String triggerId) {
        return triggerDao.getTrigger(triggerId);
    }

    /**
     * Registers a {@code Trigger} with trigger service
     * @param triggerGroup
     * @param trigger
     * @throws SchedulerException
     */
    public String registerTrigger(String triggerGroup, Trigger trigger) throws SchedulerException {
        String triggerId = triggerDao.createTrigger(triggerGroup, trigger);
        if (trigger instanceof ScheduledTrigger) {
            scheduleTrigger((ScheduledTrigger) trigger);
        }
        return triggerId;
    }

    /**
     * Disables the {@code Trigger}. If the {@code Trigger} is disabled it will NOT execute
     * @param triggerId
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void disableTrigger(String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            disableTrigger(trigger);
        } else {
            throw new TriggerNotFoundException("No trigger found with trigger id: " + triggerId);
        }
    }

    /**
     * Disables the {@code Trigger}. If the {@code Trigger} is disabled it will NOT execute
     * @param trigger
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void disableTrigger(Trigger trigger) throws SchedulerException {
        trigger.setDisabled(true);
        triggerDao.updateTrigger(trigger);
        if (trigger instanceof ScheduledTrigger) {
            unscheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Enables the {@code Trigger}
     * @param triggerId
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void enableTrigger(String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            enableTrigger(trigger);
        } else {
            throw new TriggerNotFoundException("No trigger found with trigger id: " + triggerId);
        }
    }

    /**
     * Enables the {@code Trigger}
     * @param trigger
     * @throws SchedulerException
     */
    public void enableTrigger(Trigger trigger) throws SchedulerException {
        trigger.setDisabled(false);
        triggerDao.updateTrigger(trigger);
        if (trigger instanceof ScheduledTrigger) {
            scheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Deletes/Removes the {@code Trigger}. If it is a {@code ScheduledTrigger} then it is also un-scheduled from
     * scheduler
     * @param triggerId
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void deleteTrigger(String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            deleteTrigger(trigger);
        } else {
            throw new TriggerNotFoundException("No trigger found for trigger id: " + triggerId);
        }
    }

    /**
     * Deletes/Removes the {@code Trigger}. If it is a {@code ScheduledTrigger} then it is also un-scheduled from
     * scheduler
     * @param trigger
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void deleteTrigger(Trigger trigger) throws SchedulerException {
        triggerDao.deleteTrigger(trigger);
        if (trigger instanceof ScheduledTrigger) {
            unscheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Schedules the {@code Trigger} using the scheduler
     * @param scheduledTrigger
     * @throws SchedulerException
     */
    public void scheduleTrigger(ScheduledTrigger scheduledTrigger) throws SchedulerException {
        if (!initialized.get()) throw new SchedulerException("Trigger service is not initialized. initialize() must be called before calling scheduleTrigger() method");
        Map jobDataMap = new HashMap();
        jobDataMap.put(TRIGGER_OPERATOR_KEY, this);
        jobDataMap.put(TRIGGER_KEY, scheduledTrigger);
        try {
            org.quartz.Trigger quartzTrigger = newTrigger()
                .withIdentity(triggerKey(scheduledTrigger.getId(), Scheduler.DEFAULT_GROUP))
                .withSchedule(scheduledTrigger.getScheduleBuilder())
                .usingJobData(new JobDataMap(jobDataMap))
                .startNow()
                .build();
            scheduler.scheduleQuartzJob(scheduledTrigger.getId(), Scheduler.DEFAULT_GROUP, ScheduledTriggerJob.class, quartzTrigger);
            scheduledTrigger.setQuartzTrigger(quartzTrigger);
            logger.info("Successfully scheduled {}", scheduledTrigger);
        } catch (org.quartz.SchedulerException e) {
            throw new SchedulerException("Exception occurred while scheduling trigger: " + scheduledTrigger, e);
        }
    }

    /**
     * A quartz job that is executed every time a {@code Trigger} is invoked
     */
    public static class ScheduledTriggerJob implements org.quartz.Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            TriggerOperator triggerOperator = (TriggerOperator) context.getMergedJobDataMap().get(TRIGGER_OPERATOR_KEY);
            Trigger trigger = (Trigger) context.getMergedJobDataMap().get(TRIGGER_KEY);
            try {
                triggerOperator.execute(trigger);
            } catch (Exception e) {
                throw new JobExecutionException(e);
            }
        }
    }

    /**
     * Checks if a {@code ScheduledTrigger} is scheduled in the scheduler or not
     * @param scheduledTrigger
     * @return
     * @throws SchedulerException
     */
    public boolean isScheduled(ScheduledTrigger scheduledTrigger) throws SchedulerException {
        try {
            return scheduler.isScheduled(scheduledTrigger.getId(), Scheduler.DEFAULT_GROUP);
        } catch (org.quartz.SchedulerException e) {
            throw new SchedulerException("Exception occurred while checking isScheduled() for: " + scheduledTrigger, e);
        }
    }

    /**
     * Un-schedules the {@code Trigger}
     * @param scheduledTrigger
     * @throws SchedulerException
     */
    public void unscheduleTrigger(ScheduledTrigger scheduledTrigger) throws SchedulerException {
        if (!initialized.get()) throw new SchedulerException("Trigger service is not initialized. initialize() must be called before calling unscheduleTrigger() method");
        try {
            scheduler.unscheduleQuartzJob(scheduledTrigger.getId(), Scheduler.DEFAULT_GROUP);
            scheduledTrigger.setQuartzTrigger(null);
            logger.info("Successfully unscheduled {}", scheduledTrigger);
        } catch (org.quartz.SchedulerException e) {
            throw new SchedulerException("Exception occurred while unscheduling trigger: " + scheduledTrigger, e);
        }
    }

    /**
     * Returns a list of {@code Trigger}s registered with the trigger service for the given triggerGroup
     * @param triggerGroup
     * @return
     */
    public List<Trigger> getTriggers(String triggerGroup) {
        return triggerDao.getTriggers(triggerGroup);
    }

    /**
     * Returns a list of all the {@code Trigger}s registered with the trigger service
     * @return
     */
    public List<Trigger> getTriggers() {
        return triggerDao.getTriggers();
    }

    /**
     * Executes the {@code Trigger}
     * @param triggerId
     * @throws Exception
     */
    public void execute(String triggerId) throws Exception {
        Trigger trigger = getTrigger(triggerId);
        if (trigger != null) {
            execute(trigger);
        } else {
            throw new TriggerNotFoundException(String.format("No trigger found with id: %s", triggerId));
        }
    }

    /**
     * Executes the {@code Trigger}
     * @param trigger
     * @throws Exception
     */
    public void execute(Trigger trigger) throws Exception {
        if (trigger.isDisabled()) return;
        try {
            ((Action1) trigger.getAction().newInstance()).call(trigger.getData());
        } catch (Exception e) {
            throw new Exception(String.format("Exception occurred while executing trigger '%s'", trigger), e);
        }
    }

}
