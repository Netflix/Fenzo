/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
