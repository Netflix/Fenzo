package io.mantisrx.fenzo;

import java.util.Map;

public interface TaskTrackerState {
    public Map<String, TaskTracker.ActiveTask> getAllRunningTasks();
    public Map<String, TaskTracker.ActiveTask> getAllCurrentlyAssignedTasks();
}
