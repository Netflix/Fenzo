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

import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.queues.QueuableTask;

import java.util.*;

/**
 * Determines by how many hosts the system is currently undershooting its needs, based on task assignment
 * failures. This is used in autoscaling.
 */
class NaiveShortfallEvaluator extends BaseShortfallEvaluator {

    @Override
    public Map<String, Integer> getShortfall(Set<String> vmGroupNames, Set<TaskRequest> failures, AutoScaleRules autoScaleRules) {
        // A naive approach to figuring out shortfall of hosts to satisfy the tasks that failed assignments is,
        // strictly speaking, not possible by just attempting to add up required resources for the tasks and then mapping
        // that to the number of hosts required to satisfy that many resources. Several things can make that evaluation
        // wrong, including:
        //     - tasks may have a choice to be satisfied by multiple types of hosts
        //     - tasks may have constraints (hard and soft) that alter scheduling decisions
        //
        // One approach that would work very well is by trying to schedule them all by mirroring the scheduler's logic,
        // by giving it numerous "free" resources of each type defined in the autoscale rules. Scheduling the failed tasks
        // on sufficient free resources should tell us how many resources are needed. However, we need to make sure this
        // process does not step on the original scheduler's state information. Or, if it does, it can be reliably undone.
        //
        // However, setting up the free resources is not defined yet since it involves setting up the appropriate
        // VM attributes that we have no knowledge of.
        //
        // Therefore, we fall back to a naive approach in this implementation that can work conservatively.
        // We assume that each task may need an entire new VM. This has the effect of aggressively scaling up. This
        // is better than falling short of resources, so we will go with it. The scale down will take care of things when
        // shortfall is taken care of.
        // We need to ensure, though, that we don't ask for resources for the same tasks multiple times. Therefore, we
        // maintain the list of tasks for which we have already requested resources. So, we ask again only if we notice
        // new tasks. There can be some scenarios where a resource that we asked got used up for a task other than the one
        // we asked for. Although this is unlikely in FIFO queuing of tasks, it can happen with certain task queue
        // implementations that have ordering semantics other than FIFO. So, we maintain the list of tasks with a
        // timeout and remove it from there so we catch this scenario and ask for the resources again for that task.

        if (vmGroupNames != null && failures != null && !failures.isEmpty()) {
            reset();
            return adjustAgentScaleUp(fillShortfallMap(vmGroupNames, filterFailedTasks(failures)), autoScaleRules);
        }
        else
            return Collections.emptyMap();
    }

    private Map<String, Integer> adjustAgentScaleUp(Map<String, Integer> shortfallMap, AutoScaleRules autoScaleRules) {
        Map<String, Integer> corrected = new HashMap<>(shortfallMap);
        for (Map.Entry<String, Integer> entry : shortfallMap.entrySet()) {
            AutoScaleRule rule = autoScaleRules.get(entry.getKey());
            if (rule != null) {
                int adjustedValue = rule.getShortfallAdjustedAgents(entry.getValue());
                if (adjustedValue > 0) {
                    corrected.put(entry.getKey(), adjustedValue);
                }
            }
        }
        return corrected;
    }
}
