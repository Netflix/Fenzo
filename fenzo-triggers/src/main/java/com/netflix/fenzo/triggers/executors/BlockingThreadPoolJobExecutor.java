package com.netflix.fenzo.triggers.executors;

import com.netflix.fenzo.triggers.Job;
import com.netflix.fenzo.triggers.Status;
import com.netflix.fenzo.triggers.persistence.EventDao;
import com.netflix.fenzo.triggers.Event;
import com.netflix.fenzo.triggers.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.*;

/**
 *
 */
public class BlockingThreadPoolJobExecutor implements JobExecutor {

    private static Logger logger = LoggerFactory.getLogger(BlockingThreadPoolJobExecutor.class);

    private final EventDao eventDao;
    private final ExecutorService executorService;
    private final ConcurrentMap<String,Future> futures = new ConcurrentHashMap<String, Future>();

    public BlockingThreadPoolJobExecutor(EventDao eventDao, int threadPoolSize) {
        this.eventDao = eventDao;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void execute(final Job job, final Event event) {
        final Trigger trigger = event.getTrigger();

        Future future = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (trigger.isNotificationsEnabled()) {
                        event.notifyForEventStart(event.getOwners(), event.getWatchers());
                    }

                    event.setStartTime(new Date());
                    event.setStatus(Status.IN_PROGRESS);
                    job.execute(trigger.getParameters());

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        registerFuture(event, future);

        Throwable throwable = null;
        Status eventStatus = event.getStatus();
        long timeout = trigger.getExecutionTimeoutInSeconds();

        try {
            future.get(timeout, TimeUnit.SECONDS);
            eventStatus = Status.COMPLETED;

            if (trigger.isNotificationsEnabled()) {
                event.notifyForEventEnd(event.getOwners(), event.getWatchers());
            }
        } catch (TimeoutException te) {
            eventStatus = Status.FAILED;
            throwable = new Exception(String.format("Job %s timed out after %d seconds", trigger.getJobClass(), timeout), te);
            eventStatus.setThrowable(throwable);
        } catch (ExecutionException e) {
            eventStatus = Status.FAILED;
            throwable = e.getCause();
            eventStatus.setThrowable(throwable);
        } catch (Exception e) {
            eventStatus = Status.FAILED;
            throwable = e;
            eventStatus.setThrowable(throwable);
        } finally {
            event.setEndTime(new Date());
            event.setStatus(eventStatus);
            eventDao.updateEvent(trigger.getId(), event);

            if (trigger.isNotificationsEnabled() && throwable != null) {
                event.notifyForError(event.getOwners(), event.getWatchers(), throwable);
            }
        }
    }

    @Override
    public void cancel(Job job, Event event) {
        Throwable throwable = null;
        try {
            job.cancel();
            Future future = getFuture(event);
            if (future != null) {
                future.cancel(true);
            }
        } catch (Exception e) {
            String message = String.format("Exception occurred while cancelling event %s", event);
            throwable = new Exception(message, e);
            logger.error(message, e);
        } finally {
            event.setEndTime(new Date());
            Status cancelledStatus = Status.CANCELLED;
            if (throwable != null) {
                cancelledStatus.setThrowable(throwable);
            }
            event.setStatus(cancelledStatus);
            eventDao.updateEvent(event.getTrigger().getId(), event);
        }
    }

    private void registerFuture(Event event, Future future) {
        if (futures.putIfAbsent(event.getId(), future) != null) {
            synchronized (futures) {
                futures.put(event.getId(), future);
            }
        }
    }

    private Future getFuture(Event event) {
        return futures.remove(event.getId());
    }
}
