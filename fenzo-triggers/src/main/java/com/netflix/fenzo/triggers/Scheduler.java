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

    static Scheduler getInstance() {
        return SchedulerHolder.INSTANCE;
    }

    private Scheduler() {
    }

    synchronized void startScheduler(int threadPoolSize) throws SchedulerException {
        if (quartzScheduler == null) {
            Properties props = new Properties();
            props.setProperty("org.quartz.threadPool.threadCount", String.format("%d", threadPoolSize));
            StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory(props);
            quartzScheduler = stdSchedulerFactory.getScheduler();
            quartzScheduler.start();
        }
    }

    void stopScheduler() throws SchedulerException {
        quartzScheduler.shutdown();
    }

    void stopScheduler(boolean waitForJobsToComplete) throws SchedulerException {
        quartzScheduler.shutdown(waitForJobsToComplete);
    }

    void scheduleQuartzJob(String jobId, String jobGroup, Class<? extends Job> jobClass, Trigger trigger) throws SchedulerException {
        JobDetail job = newJob(jobClass).withIdentity(jobId, jobGroup).build();
        quartzScheduler.scheduleJob(job, trigger);
    }

    void unscheduleQuartzJob(String jobId, String jobGroup) throws SchedulerException {
        quartzScheduler.unscheduleJob(TriggerKey.triggerKey(jobId, jobGroup));
    }

}
