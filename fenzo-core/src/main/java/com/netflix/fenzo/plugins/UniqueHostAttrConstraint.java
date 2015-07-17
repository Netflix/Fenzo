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
 * A unique constraint evaluator that constrains tasks according to a given host attribute. Use this evaluator to
 * constrain a given set of tasks (co-tasks) by assigning them to hosts such that each task is assigned to a host
 * that has a different value for the host attribute you specify.
 *
 * For example, if you specify the host attribute {@code ZoneAttribute}, the evaluator will place the co-tasks
 * such that each task is in a different zone. Note that because of this, if more tasks are submitted than there
 * are zones available for them, the excess tasks will not be assigned to any hosts. If instead you want to
 * implement a load balancing strategy, you can do so by converting this into a <em>soft</em> constraint by
 * passing it in to the {@link AsSoftConstraint} constructor.
 *
 * If you construct this evaluator without passing in a host attribute name, it will use the host name as the
 * host attribute by which it uniquely identifies hosts.
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

    /**
     * Returns the name of this constraint evaluator as a String in the form of the name of this class followed
     * by a dash followed by the host attribute name that this evaluator uses to uniquely identify the host.
     *
     * @return the name of this constraint evaluator
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     *
     * @param taskRequest
     * @param targetVM
     * @param taskTrackerState
     * @return
     */
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
