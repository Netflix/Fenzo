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
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTracker;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.functions.Func1;

import java.util.Set;

/**
 * A unique constraint evaluator to constrain tasks by a given host attribute.
 * <P>
 *     This can be used to constrain a given set of tasks (co-tasks) are assigned hosts with unique
 * value for the given host attribute. For example, given a host attribute <code>ZoneAttribute</code>,
 * the co-tasks would be placed one one task per zone. This implies that any co-tasks submitted in greater
 * number than the number of zones will not get assigned any hosts. Instead, if a load balancing strategy
 * is to be obtained, the <code>asSoftConstraint()</code> provides the conversion.
 * </P>
 * <P>
 *     When constructed without a host attribute name, this constraint evaluator uses host names as the attribute
 *     for the unique constraint.
 * </P>
 */
public class UniqueHostAttrConstraint implements ConstraintEvaluator {
    private final Func1<String, Set<String>> coTasksGetter;
    private final String hostAttributeName;
    private final String name;

    public UniqueHostAttrConstraint(Func1<String, Set<String>> coTasksGetter) {
        this(coTasksGetter, AttributeUtilities.DEFAULT_ATTRIBUTE);
    }

    public UniqueHostAttrConstraint(Func1<String, Set<String>> coTasksGetter, String hostAttributeName) {
        this.coTasksGetter = coTasksGetter;
        this.hostAttributeName = hostAttributeName;
        this.name = UniqueHostAttrConstraint.class.getName()+"-"+hostAttributeName;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Set<String> coTasks = coTasksGetter.call(taskRequest.getId());
        String targetHostAttrVal = AttributeUtilities.getAttrValue(targetVM.getCurrAvailableResources(), hostAttributeName);
        if(targetHostAttrVal==null || targetHostAttrVal.isEmpty()) {
            return new Result(false, hostAttributeName + " attribute unavailable on host " + targetVM.getCurrAvailableResources().hostname());
        }
        for(String coTask: coTasks) {
            TaskTracker.ActiveTask activeTask = taskTrackerState.getAllRunningTasks().get(coTask);
            if(activeTask==null)
                activeTask = taskTrackerState.getAllCurrentlyAssignedTasks().get(coTask);
            if(activeTask!=null) {
                String usedAttrVal = AttributeUtilities.getAttrValue(activeTask.getTotalLease(), hostAttributeName);
                if(usedAttrVal==null || usedAttrVal.isEmpty())
                    return new Result(false, hostAttributeName+" attribute unavailable on host " + activeTask.getTotalLease().hostname() +
                            " running co-task " + coTask);
                if(usedAttrVal.equals(targetHostAttrVal)) {
                    return new Result(false, hostAttributeName+" " + targetHostAttrVal + " already used for another co-task " + coTask);
                }
            }
        }
        return new Result(true, "");
    }

}
