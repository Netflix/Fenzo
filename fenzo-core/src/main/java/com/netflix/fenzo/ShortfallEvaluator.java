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

import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.queues.QueuableTask;

import java.util.List;
import java.util.Map;
import java.util.Set;

/* package */ interface ShortfallEvaluator {

    void setTaskToClustersGetter(Func1<QueuableTask, List<String>> getter);

    /**
     * Get number of VMs of shortfall for each VM Group for the given VM Group names and set of failed tasks.
     * @param vmGroupNames VM Group names for which autoscale rules are setup, same as the autoscale rule name.
     * @param failures The tasks that failed assignment due to which VM shortfall must be evaluated.
     * @param autoScaleRules The current set of autoscale rules.
     * @return A map with keys containing VM group names (same as autoscale rule names) and values containing the
     *         number of VMs that may be added to address the shortfall.
     */
    Map<String, Integer> getShortfall(Set<String> vmGroupNames, Set<TaskRequest> failures, AutoScaleRules autoScaleRules);

    void setTaskSchedulingService(TaskSchedulingService schedulingService);
}
