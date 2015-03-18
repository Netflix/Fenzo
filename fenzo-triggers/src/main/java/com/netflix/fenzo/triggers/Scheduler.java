package com.netflix.fenzo.triggers;

import org.quartz.CronTrigger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Map;
import java.util.Properties;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;


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

    org.quartz.CronTrigger scheduleQuartzJob(Class<? extends org.quartz.Job> jobClass, String jobId, String jobGroup, Map jobDataMap, String cron) throws SchedulerException {
        JobDetail job = newJob(jobClass).withIdentity(jobId, jobGroup).build();
        CronTrigger trigger = newTrigger()
                .withIdentity(triggerKey(jobId, jobGroup))
                .withSchedule(cronSchedule(cron))
                .usingJobData(new JobDataMap(jobDataMap))
                .startNow()
                .build();
        quartzScheduler.scheduleJob(job, trigger);
        return trigger;
    }

    void unscheduleQuartzJob(String jobId, String jobGroup) throws SchedulerException {
        quartzScheduler.unscheduleJob(TriggerKey.triggerKey(jobId, jobGroup));
    }

}
