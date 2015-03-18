package com.netflix.fenzo;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TaskTracker {

    public static class ActiveTask {
        private TaskRequest taskRequest;
        private AssignableVirtualMachine avm;
        public ActiveTask(TaskRequest taskRequest, AssignableVirtualMachine avm) {
            this.taskRequest = taskRequest;
            this.avm = avm;
        }
        public TaskRequest getTaskRequest() {
            return taskRequest;
        }
        public VirtualMachineLease getTotalLease() {
            return avm.getCurrTotalLease();
        }
    }

    private final ConcurrentMap<String, ActiveTask> runningTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ActiveTask> assignedTasks = new ConcurrentHashMap<>();

    // package scoped
    TaskTracker() {
    }

    boolean addRunningTask(TaskRequest request, AssignableVirtualMachine avm) {
        return runningTasks.putIfAbsent(request.getId(), new ActiveTask(request, avm)) == null;
    }

    boolean removeRunningTask(String taskId) {
        return runningTasks.remove(taskId)!=null;
    }

    Map<String, ActiveTask> getAllRunningTasks() {
        return Collections.unmodifiableMap(runningTasks);
    }

    boolean addAssignedTask(TaskRequest request, AssignableVirtualMachine avm) {
        return assignedTasks.putIfAbsent(request.getId(), new ActiveTask(request, avm)) == null;
    }

    void clearAssignedTasks() {
        assignedTasks.clear();
    }

    Map<String, ActiveTask> getAllAssignedTasks() {
        return Collections.unmodifiableMap(assignedTasks);
    }
}
