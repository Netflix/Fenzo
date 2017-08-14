/*
 * Copyright 2017 Netflix, Inc.
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

import com.netflix.fenzo.queues.InternalTaskQueue;
import com.netflix.fenzo.queues.InternalTaskQueues;
import com.netflix.fenzo.queues.QueuableTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements an optimal evaluation of the shortfall of the right VMs needed for cluster scale up.
 * In a cluster with multiple VM groups, this evaluator is able to determine the minimum number of VMs of each group
 * required to satisfy the resource demands from pending tasks at the end of a scheduling iteration.
 * <P>
 * This evaluator can only be used with {@link TaskSchedulingService}, it cannot be used when directly using
 * {@link TaskScheduler}. The latter uses {@link NaiveShortfallEvaluator} for shortfall analysis.
 * <P>
 * This evaluator works by requesting a "pseudo" scheduling iteration from {@link TaskSchedulingService} with a
 * new queue that is cloned from the original queue being used and adding to it only the tasks that failed assignments.
 * {@link TaskSchedulingService} performs the pseudo scheduling run by creating the appropriate number of pseudo VMs
 * for each group of VMs. For this, the autoscale rules are consulted to ensure the scale up will honor the maximum VMs
 * for each group.
 * <P>
 * The pseudo scheduling run performs an entire scheduling iteration using the cloned queue and pseudo VMs in addition
 * to any new VM leases that have been added since previous scheduling iteration. This will invoke any and all task
 * constraints as well as fitness function setup in the scheduler. The scheduling result is used to determine the
 * number of VMs in each group and then the results are discarded. As expected, the pseudo scheduling run has no impact
 * on the real scheduling assignments made.
 * <P>
 * Tasks for which scale up is requested by this evaluator are remembered and not requested again until certain delay.
 */
class OptimizingShortfallEvaluator extends BaseShortfallEvaluator {

    @Override
    public Map<String, Integer> getShortfall(Set<String> vmGroupNames, Set<TaskRequest> failures, AutoScaleRules autoScaleRules) {
        if (schedulingService == null || failures == null || failures.isEmpty())
            return Collections.emptyMap();

        final List<TaskRequest> filteredTasks = filterFailedTasks(failures);
        final Map<String, Integer> shortfallTasksPerGroup = fillShortfallMap(vmGroupNames, filteredTasks);
        if (shortfallTasksPerGroup.isEmpty())
            return Collections.emptyMap();

        if (schedulingService.isShutdown())
            return Collections.emptyMap();

        final InternalTaskQueue taskQueue = createAndFillAlternateQueue(filteredTasks);
        return schedulingService.requestPseudoScheduling(taskQueue, shortfallTasksPerGroup);
    }

    private InternalTaskQueue createAndFillAlternateQueue(List<TaskRequest> shortfallTasks) {
        final InternalTaskQueue taskQueue = InternalTaskQueues.createQueueOf(schedulingService.getQueue());
        for (TaskRequest t: shortfallTasks) {
            taskQueue.queueTask((QueuableTask) t);
        }
        return taskQueue;
    }
}
