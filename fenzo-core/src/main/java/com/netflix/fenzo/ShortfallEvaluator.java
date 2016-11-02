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
class ShortfallEvaluator {
    private static final long TOO_OLD_THRESHOLD_MILLIS = 10 * 60000; // 15 mins
    private final TaskScheduler phantomTaskScheduler;
    private final Map<String, Long> requestedForTasksSet = new HashMap<>();
    private volatile Func1<QueuableTask, List<String>> taskToClustersGetter = null;

    ShortfallEvaluator(TaskScheduler phantomTaskScheduler) {
        this.phantomTaskScheduler = phantomTaskScheduler;
    }

    void setTaskToClustersGetter(Func1<QueuableTask, List<String>> getter) {
        taskToClustersGetter = getter;
    }

    Map<String, Integer> getShortfall(Set<String> attrKeys, Set<TaskRequest> failures) {
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
        // Therefore, we fall back to a naive approach that is conservative enough to work for now.
        // We assume that each task may need an entire new VM. This has the effect of aggressively scaling up. This
        // is better than falling short of resources, so we will go with it. The scale down will take care of things when
        // shortfall is taken care of.
        // We need to ensure, though, that we don't ask for resources for the same tasks multiple times. That is, we
        // maintain the list of tasks for which we have already requested resources. So, we ask again only if we notice
        // new tasks. There can be some scenarios where a resource that we asked got used up for a task other than the one
        // we asked for. Although, this is unlikely in FIFO queuing of tasks. So, we maintain the list of tasks with a
        // timeout and unqueueTask it from there so we catch this scenario and ask for the resources again for that task.

        final HashMap<String, Integer> shortfallMap = new HashMap<>();
        if(attrKeys!=null && failures!=null && !failures.isEmpty()) {
            removeOldInserts();
            long now = System.currentTimeMillis();
            for(TaskRequest r: failures) {
                String tid = r.getId();
                if(requestedForTasksSet.get(tid) == null) {
                    requestedForTasksSet.put(tid, now);
                    fillShortfallMap(shortfallMap, attrKeys, r);
                }
            }
        }
        return shortfallMap;
    }

    private void fillShortfallMap(HashMap<String, Integer> shortfallMap, Set<String> attrKeys, TaskRequest r) {
        for (String k: attrKeys) {
            if (matchesTask(r, k)) {
                if (shortfallMap.get(k) == null)
                    shortfallMap.put(k, 1);
                else
                    shortfallMap.put(k, shortfallMap.get(k) + 1);
            }
        }
    }

    private boolean matchesTask(TaskRequest r, String k) {
        if (!(r instanceof QueuableTask) || taskToClustersGetter == null)
            return true;
        final List<String> strings = taskToClustersGetter.call((QueuableTask) r);
        if (strings != null && !strings.isEmpty()) {
            for (String s: strings)
                if(k.equals(s))
                    return true;
            return false; // doesn't match
        }
        // matches anything
        return true;
    }

    private void removeOldInserts() {
        long tooOld = System.currentTimeMillis() - TOO_OLD_THRESHOLD_MILLIS;
        Set<String> tasks = new HashSet<>(requestedForTasksSet.keySet());
        for(String t: tasks) {
            if(requestedForTasksSet.get(t) < tooOld)
                requestedForTasksSet.remove(t);
        }
    }
}
