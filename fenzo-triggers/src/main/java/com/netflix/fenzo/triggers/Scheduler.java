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

import org.quartz.*;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

import static org.quartz.JobBuilder.newJob;


/**
 * Basically, a wrapper over Quartz scheduler. Not intended to be used by classes outside this package
 *
 */
class Scheduler {

    static final String DEFAULT_GROUP = "DEFAULT_GROUP";
    private org.quartz.Scheduler quartzScheduler;

    private static class SchedulerHolder {
        private static final Scheduler INSTANCE = new Scheduler();
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    static Scheduler getInstance() {
        return SchedulerHolder.INSTANCE;
    }

    private Scheduler() {
    }

    /**
     * @warn method description missing
     * @warn parameter threadPoolSize description missing
     * @warn exception SchedulerException description missing
     *
     * @param threadPoolSize
     * @throws SchedulerException
     */
    synchronized void startScheduler(int threadPoolSize) throws SchedulerException {
        if (quartzScheduler == null) {
            Properties props = new Properties();
            props.setProperty("org.quartz.threadPool.threadCount", String.format("%d", threadPoolSize));
            StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory(props);
            quartzScheduler = stdSchedulerFactory.getScheduler();
            quartzScheduler.start();
        }
    }

    /**
     * @warn method description missing
     * @warn exception SchedulerException description missing
     *
     * @throws SchedulerException
     */
    void stopScheduler() throws SchedulerException {
        quartzScheduler.shutdown();
    }

    /**
     * @warn method description missing
     * @warn parameter waitForJobsToComplete description missing
     * @warn exception SchedulerException description missing
     *
     * @param waitForJobsToComplete
     * @throws SchedulerException
     */
    void stopScheduler(boolean waitForJobsToComplete) throws SchedulerException {
        quartzScheduler.shutdown(waitForJobsToComplete);
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     * @warn exception SchedulerException description missing
     *
     * @param jobId
     * @param jobGroup
     * @param jobClass
     * @param trigger
     * @throws SchedulerException
     */
    void scheduleQuartzJob(String jobId, String jobGroup, Class<? extends Job> jobClass, Trigger trigger) throws SchedulerException {
        JobDetail job = newJob(jobClass).withIdentity(jobId, jobGroup).build();
        quartzScheduler.scheduleJob(job, trigger);
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     * @warn exception SchedulerException description missing
     *
     * @param jobId
     * @param jobGroup
     * @throws SchedulerException
     */
    void unscheduleQuartzJob(String jobId, String jobGroup) throws SchedulerException {
        quartzScheduler.unscheduleJob(TriggerKey.triggerKey(jobId, jobGroup));
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     * @warn exception SchedulerException description missing
     *
     * @param jobId
     * @param jobGroup
     * @throws SchedulerException
     * @return
     */
    boolean isScheduled(String jobId, String jobGroup) throws SchedulerException {
        return quartzScheduler.checkExists(new JobKey(jobId, jobGroup));
    }

}
