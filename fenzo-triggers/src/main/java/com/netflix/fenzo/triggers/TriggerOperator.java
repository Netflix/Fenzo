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
import com.netflix.fenzo.triggers.persistence.TriggerDao;
import org.quartz.JobDataMap;
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

import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

/**
 *
 */
public class TriggerOperator {

    private static final String TRIGGER_KEY = "trigger";
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
        }
        for (Iterator<Trigger> iterator = getTriggers().iterator(); iterator.hasNext();) {
            Trigger trigger = iterator.next();
            if (!trigger.isDisabled() && trigger instanceof ScheduledTrigger) {
                scheduleTrigger((ScheduledTrigger) trigger);
            }
        }
    }

    /**
     * Returns the {@code Trigger} based on the unique trigger id
     * @param triggerId
     * @return
     */
    public Trigger getTrigger(String triggerGroup, String triggerId) {
        return triggerDao.getTrigger(triggerGroup, triggerId);
    }

    /**
     * Registers a {@code Trigger} with trigger service
     * @param triggerGroup
     * @param trigger
     * @throws SchedulerException
     */
    public void registerTrigger(String triggerGroup, Trigger trigger) throws SchedulerException {
        triggerDao.createTrigger(triggerGroup, trigger);
        if (trigger instanceof ScheduledTrigger) {
            scheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Disables the {@code Trigger}. If the {@code Trigger} is disabled it will NOT execute
     * @param triggerGroup
     * @param triggerId
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void disableTrigger(String triggerGroup, String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerGroup, triggerId);
        if (trigger != null) {
            disableTrigger(triggerGroup, trigger);
        } else {
            throw new TriggerNotFoundException("No trigger found with trigger id: " + triggerId);
        }
    }

    /**
     * Disables the {@code Trigger}. If the {@code Trigger} is disabled it will NOT execute
     * @param triggerGroup
     * @param trigger
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void disableTrigger(String triggerGroup, Trigger trigger) throws SchedulerException {
        trigger.setDisabled(true);
        triggerDao.updateTrigger(triggerGroup, trigger);
        if (trigger instanceof ScheduledTrigger) {
            unscheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Enables the {@code Trigger}
     * @param triggerGroup
     * @param triggerId
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void enableTrigger(String triggerGroup, String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerGroup, triggerId);
        if (trigger != null) {
            enableTrigger(triggerGroup, trigger);
        } else {
            throw new TriggerNotFoundException("No trigger found with trigger id: " + triggerId);
        }
    }

    /**
     * Enables the {@code Trigger}
     * @param triggerGroup
     * @param trigger
     * @throws SchedulerException
     */
    public void enableTrigger(String triggerGroup, Trigger trigger) throws SchedulerException {
        trigger.setDisabled(false);
        triggerDao.updateTrigger(triggerGroup, trigger);
        if (trigger instanceof ScheduledTrigger) {
            scheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Deletes/Removes the {@code Trigger}. If it is a {@code ScheduledTrigger} then it is also un-scheduled from
     * scheduler
     * @param triggerGroup
     * @param triggerId
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void deleteTrigger(String triggerGroup, String triggerId) throws TriggerNotFoundException, SchedulerException {
        Trigger trigger = getTrigger(triggerGroup, triggerId);
        if (trigger != null) {
            deleteTrigger(triggerGroup, trigger);
        } else {
            throw new TriggerNotFoundException("No trigger found for trigger id: " + triggerId);
        }
    }

    /**
     * Deletes/Removes the {@code Trigger}. If it is a {@code ScheduledTrigger} then it is also un-scheduled from
     * scheduler
     * @param triggerGroup
     * @param trigger
     * @throws TriggerNotFoundException
     * @throws SchedulerException
     */
    public void deleteTrigger(String triggerGroup, Trigger trigger) throws SchedulerException {
        triggerDao.deleteTrigger(triggerGroup, trigger);
        if (trigger instanceof ScheduledTrigger) {
            unscheduleTrigger((ScheduledTrigger) trigger);
        }
    }

    /**
     * Schedules the {@code Trigger} using the scheduler
     * @param scheduledTrigger
     * @throws SchedulerException
     */
    protected void scheduleTrigger(ScheduledTrigger scheduledTrigger) throws SchedulerException {
        if (!initialized.get()) throw new SchedulerException("Trigger service is not initialized. initialize() must be called before calling scheduleTrigger() method");
        Map jobDataMap = new HashMap();
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
    protected class ScheduledTriggerJob implements org.quartz.Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Trigger trigger = (Trigger) context.getMergedJobDataMap().get(TRIGGER_KEY);
            try {
                TriggerOperator.this.execute(trigger);
            } catch (Exception e) {
                throw new JobExecutionException(e);
            }
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
     * @param trigger
     * @throws Exception
     */
    public void execute(Trigger trigger) throws Exception {
        try {
            trigger.getAction().call(trigger.getData());
        } catch (Exception e) {
            throw new Exception(String.format("Exception occurred while executing trigger '%s'", trigger), e);
        }
    }

}
