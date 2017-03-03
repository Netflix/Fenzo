package com.netflix.fenzo.queues;

import com.netflix.fenzo.AssignmentFailure;

/**
 * A wrapper object containing a task, and optionally an assignment failure for this task.
 */
public class Assignable<T> {

    private final T task;
    private final AssignmentFailure assignmentFailure;

    private Assignable(T task, AssignmentFailure assignmentFailure) {
        this.task = task;
        this.assignmentFailure = assignmentFailure;
    }

    public T getTask() {
        return task;
    }

    public boolean hasFailure() {
        return assignmentFailure != null;
    }

    public AssignmentFailure getAssignmentFailure() {
        return assignmentFailure;
    }

    public static <T> Assignable<T> success(T task) {
        return new Assignable<>(task, null);
    }

    public static <T> Assignable<T> error(T task, AssignmentFailure assignmentFailure) {
        return new Assignable<>(task, assignmentFailure);
    }
}
