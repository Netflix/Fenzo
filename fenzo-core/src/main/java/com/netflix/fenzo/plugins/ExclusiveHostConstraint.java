package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;

import java.util.Collection;

/**
 * A constraint to ensure that the host is exclusively assigned to the given job.
 * This class cannot be extended. It is given a special treatment by the TaskScheduler in order
 * to achieve its goals. Normally, only the constraints of the new task being assigned are evaluated.
 * However, if an already assigned task has this constraint, then the host fails the constraint check as well.
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
