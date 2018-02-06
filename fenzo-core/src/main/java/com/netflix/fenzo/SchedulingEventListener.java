package com.netflix.fenzo;

/**
 * A callback API providing notification about Fenzo task placement decisions during the scheduling process.
 */
public interface SchedulingEventListener {

    /**
     * Called before a new scheduling iteration is started.
     */
    void onScheduleStart();

    /**
     * Called when a new task placement decision is made (a task gets resources allocated on a server).
     *
     * @param taskAssignmentResult task assignment result
     */
    void onAssignment(TaskAssignmentResult taskAssignmentResult);

    /**
     * Called when the scheduling iteration completes.
     */
    void onScheduleFinish();
}
