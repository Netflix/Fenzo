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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

abstract class BaseShortfallEvaluator implements ShortfallEvaluator {
    private static final long TOO_OLD_THRESHOLD_MILLIS = 10 * 60000; // should this be configurable?

    private volatile Func1<QueuableTask, List<String>> taskToClustersGetter = null;
    private final Map<String, Long> requestedForTasksSet = new HashMap<>();

    protected TaskSchedulingService schedulingService = null;

    @Override
    public void setTaskToClustersGetter(Func1<QueuableTask, List<String>> getter) {
        this.taskToClustersGetter = getter;
    }

    @Override
    public abstract Map<String, Integer> getShortfall(Set<String> vmGroupNames, Set<TaskRequest> failures, AutoScaleRules autoScaleRules);

    @Override
    public void setTaskSchedulingService(TaskSchedulingService schedulingService) {
        this.schedulingService = schedulingService;
    }

    protected void reset() {
        long tooOld = System.currentTimeMillis() - TOO_OLD_THRESHOLD_MILLIS;
        Set<String> tasks = new HashSet<>(requestedForTasksSet.keySet());
        for (String t : tasks) {
            if (requestedForTasksSet.get(t) < tooOld)
                requestedForTasksSet.remove(t);
        }
    }

    protected List<TaskRequest> filterFailedTasks(Collection<TaskRequest> original) {
        if (original == null || original.isEmpty())
            return Collections.emptyList();
        long now = System.currentTimeMillis();
        return original.stream()
                .filter(t -> requestedForTasksSet.putIfAbsent(t.getId(), now) == null)
                .collect(Collectors.toList());
    }

    protected Map<String, Integer> fillShortfallMap(Set<String> attrKeys, Collection<TaskRequest> requests) {
        Map<String, Integer> shortfallMap = new HashMap<>();
        if (requests != null && !requests.isEmpty()) {
            for (TaskRequest r: requests) {
                for (String k : attrKeys) {
                    if (matchesTask(r, k)) {
                        if (shortfallMap.get(k) == null)
                            shortfallMap.put(k, 1);
                        else
                            shortfallMap.put(k, shortfallMap.get(k) + 1);
                    }
                }
            }
        }
        return shortfallMap;
    }

    private boolean matchesTask(TaskRequest r, String k) {
        if (!(r instanceof QueuableTask) || taskToClustersGetter == null)
            return true;
        final List<String> strings = taskToClustersGetter.call((QueuableTask) r);
        if (strings != null && !strings.isEmpty()) {
            for (String s : strings)
                if (k.equals(s))
                    return true;
            return false; // doesn't match
        }
        // matches anything
        return true;
    }
}
