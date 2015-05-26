package com.netflix.fenzo;

import rx.functions.Func1;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Used by AutoScaler to evaluate number of hosts that we are falling short based on assignment failures
class ShortfallEvaluator {
    private static final long TOO_OLD_THRESHOLD_MILLIS = 15 * 60000; // 15 mins
    private final TaskScheduler phantomTaskScheduler;
    private final Map<String, Long> requestedForTasksSet = new HashMap<>();

    ShortfallEvaluator(TaskScheduler phantomTaskScheduler) {
        this.phantomTaskScheduler = phantomTaskScheduler;
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
        // slave attributes that we have no knowledge of.
        //
        // Therefore, we fall back to a naive approach that is conservative enough to work for now.
        // We assume that each task may need an entire new slave. This has the effect of aggressively scaling up. This
        // is better than falling short of resources, so we will go with it. The scale down will take care of things when
        // shortfall is taken care of.
        // We need to ensure, though, that we don't ask for resources for the same tasks multiple times. That is, we
        // maintain the list of tasks for which we have already requested resources. So, we ask again only if we notice
        // new tasks. There can be some scenarios where a resource that we asked got used up for a task other than the one
        // we asked for. Although, this is unlikely in FIFO queuing of tasks. So, we maintain the list of tasks with a
        // timeout and remove it from there so we catch this scenario and ask for the resources again for that task.

        final HashMap<String, Integer> shortfallMap = new HashMap<>();
        if(attrKeys!=null && failures!=null && !failures.isEmpty()) {
            removeOldInserts();
            int shortfall=0;
            long now = System.currentTimeMillis();
            for(TaskRequest r: failures) {
                String tid = r.getId();
                if(requestedForTasksSet.get(tid) == null) {
                    requestedForTasksSet.put(tid, now);
                    shortfall++;
                }
            }
            for(String key: attrKeys) {
                shortfallMap.put(key, shortfall);
            }
        }
        return shortfallMap;
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
