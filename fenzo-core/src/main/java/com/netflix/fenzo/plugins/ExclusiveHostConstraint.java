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
 * This constraint ensures that the host is exclusively assigned to the given job. You may not extend this
 * class.
 * <p>
 * The task scheduler gives this constraint special treatment: While usually the scheduler only evaluates the
 * constraints of a new task, if an already-assigned task has an {@code ExclusiveHostRestraint} then the host on
 * which the task has been assigned fails this exclusive host constraint check as well.
 */
public final class ExclusiveHostConstraint implements ConstraintEvaluator {
    /**
     * Returns the name of this class as a String.
     *
     * @return the name of this class
     */
    @Override
    public String getName() {
        return ExclusiveHostConstraint.class.getName();
    }

    /**
     * Determines whether the prospective host already has tasks either running on it or assigned to be run on
     * it, and returns a false Result if either of those things is the case.
     *
     * @param taskRequest      describes the task to be considered for assignment to the host
     * @param targetVM         describes the host to be considered for accepting the task
     * @param taskTrackerState describes the state of tasks assigned to or running on hosts throughout the
     *                         system
     * @return an unsuccessful Result if the target already has running tasks or assigned tasks, or a successful
     *         Result otherwise
     */
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
