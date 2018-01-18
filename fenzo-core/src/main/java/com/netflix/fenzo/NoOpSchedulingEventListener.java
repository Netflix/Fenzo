package com.netflix.fenzo;

class NoOpSchedulingEventListener implements SchedulingEventListener {

    static final SchedulingEventListener INSTANCE = new NoOpSchedulingEventListener();

    @Override
    public void onScheduleStart() {
    }

    @Override
    public void onAssignment(TaskAssignmentResult taskAssignmentResult) {
    }

    @Override
    public void onScheduleFinish() {
    }
}
