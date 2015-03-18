package com.netflix.fenzo.triggers.executors;

import com.netflix.fenzo.triggers.Job;
import com.netflix.fenzo.triggers.persistence.EventDao;
import com.netflix.fenzo.triggers.Event;
import com.netflix.fenzo.triggers.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public class SimpleThreadPoolTriggerExecutor extends TriggerExecutorSupport {

    private static Logger logger = LoggerFactory.getLogger(SimpleThreadPoolTriggerExecutor.class);

    private final JobExecutor jobExecutor;
    private final ExecutorService executorService;

    public SimpleThreadPoolTriggerExecutor(JobExecutor jobExecutor, EventDao eventDao, int threadPoolSize) {
        super(eventDao);
        this.jobExecutor = jobExecutor;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public Event execute(final Trigger trigger) throws Exception {

        if (trigger.isDisabled()) throw new Exception(String.format("Trigger %s is disabled or deleted", trigger));
        if (!trigger.isConcurrentEventsEnabled() && isTriggerExecuting(trigger)) return null;

        final Job job = createJobInstance(trigger);
        final Event event = createEvent(trigger, job);

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                jobExecutor.execute(job, event);
            }
        });

        return event;
    }

    @Override
    public void cancel(Event event) {
        try {
            Trigger trigger = event.getTrigger();
            Job job = event.getJob();
            jobExecutor.cancel(job, event);
            if (trigger.isNotificationsEnabled()) {
                event.notifyForEventCancel(event.getOwners(), event.getWatchers());
            }
        } catch (Exception e) {
            logger.error("Exception occurred while cancelling event {}", event, e);
        }
    }
}
