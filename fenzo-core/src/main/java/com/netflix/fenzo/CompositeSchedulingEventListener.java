package com.netflix.fenzo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

class CompositeSchedulingEventListener implements SchedulingEventListener {

    private static final Logger logger = LoggerFactory.getLogger(CompositeSchedulingEventListener.class);

    private final List<SchedulingEventListener> listeners;

    private CompositeSchedulingEventListener(Collection<SchedulingEventListener> listeners) {
        this.listeners = new ArrayList<>(listeners);
    }

    @Override
    public void onScheduleStart() {
        safely(SchedulingEventListener::onScheduleStart);
    }

    @Override
    public void onAssignment(TaskAssignmentResult taskAssignmentResult) {
        safely(listener -> listener.onAssignment(taskAssignmentResult));
    }

    @Override
    public void onScheduleFinish() {
        safely(SchedulingEventListener::onScheduleFinish);
    }

    private void safely(Consumer<SchedulingEventListener> action) {
        listeners.forEach(listener -> {
            try {
                action.accept(listener);
            } catch (Exception e) {
                logger.warn("Scheduling event dispatching error: {} -> {}", listener.getClass().getSimpleName(), e.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.debug("Details", e);
                }
            }
        });
    }

    static SchedulingEventListener of(Collection<SchedulingEventListener> listeners) {
        if (listeners.isEmpty()) {
            return NoOpSchedulingEventListener.INSTANCE;
        }
        return new CompositeSchedulingEventListener(listeners);
    }
}
