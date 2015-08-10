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
 * A unique constraint evaluator that constrains tasks according to a given host attribute. Use this evaluator
 * to constrain a given set of tasks (co-tasks) by assigning them to hosts such that each task is assigned to a
 * host that has a different value for the host attribute you specify.
 * <p>
 * For example, if you specify the host attribute {@code ZoneAttribute}, the evaluator will place the co-tasks
 * such that each task is in a different zone. Note that because of this, if more tasks are submitted than there
 * are zones available for them, the excess tasks will not be assigned to any hosts. If instead you want to
 * implement a load balancing strategy, use {@link BalancedHostAttrConstraint} instead.
 * <p>
 * If you construct this evaluator without passing in a host attribute name, it will use the host name as the
 * host attribute by which it uniquely identifies hosts.
 */
public class UniqueHostAttrConstraint implements ConstraintEvaluator {
    private final Func1<String, Set<String>> coTasksGetter;
    private final String hostAttributeName;
    private final String name;

    /**
     * Create this constraint evaluator with the given co-tasks of a group. This is equivalent to
     * {@code UniqueHostAttrConstraint(coTasksGetter, null)}.
     *
     * @param coTasksGetter a single-argument function that, given a task ID, returns the set of task IDs of its
     *                      co-tasks
     */
    public UniqueHostAttrConstraint(Func1<String, Set<String>> coTasksGetter) {
        this(coTasksGetter, AttributeUtilities.DEFAULT_ATTRIBUTE);
    }

    /**
     * Create this constraint evaluator with the given co-tasks of a group and a given host attribute name.
     *
     * @param coTasksGetter a single-argument function that, given a task ID, returns the set of task IDs of its
     *                      co-tasks
     * @param hostAttributeName the name of the host attribute whose value is to be unique for each co-task's
     *                          host assignment. If this is {@code null}, this indicates that the host name is
     *                          to be unique for each co-task's host assignment.
     */
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
     * Determines whether a particular target host is appropriate for a particular task request by rejecting any
     * host that has the same value for the unique constraint attribute as another host that is already assigned
     * a co-task of the specified task request.
     *
     * @param taskRequest      describes the task being considered for assignment to the host
     * @param targetVM         describes the host being considered as a target for the task
     * @param taskTrackerState describes the state of tasks previously assigned or already running throughout
     *                         the system
     * @return a successful Result if the target does not have the same value for its unique constraint
     *         attribute as another host that has already been assigned a co-task of {@code taskRequest}, or an
     *         unsuccessful Result otherwise
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
