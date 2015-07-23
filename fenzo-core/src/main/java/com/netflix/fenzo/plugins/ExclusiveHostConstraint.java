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

package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;

import java.util.Collection;

/**
 * A constraint implementation to ensure that the host is exclusively assigned to the given job.
 * Ensure that a VM that is assigned to a task with this constraint is not assigned to any other task until the
 * former task completes.
 */
public final class ExclusiveHostConstraint implements ConstraintEvaluator {
    @Override
    public String getName() {
        return ExclusiveHostConstraint.class.getName();
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Collection<TaskRequest> runningTasks = targetVM.getRunningTasks();
        if(runningTasks!=null && !runningTasks.isEmpty())
            return new Result(false, "Already has " + runningTasks.size() + " tasks running on it");
        Collection<TaskAssignmentResult> tasksCurrentlyAssigned = targetVM.getTasksCurrentlyAssigned();
        if(tasksCurrentlyAssigned!=null && !tasksCurrentlyAssigned.isEmpty())
            return new Result(false, "Already has " + tasksCurrentlyAssigned.size() + " assigned on it");
        return new Result(true, "");
    }
}
