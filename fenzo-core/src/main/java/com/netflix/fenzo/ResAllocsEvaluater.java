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

import com.netflix.fenzo.AssignmentFailure;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTracker;
import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.sla.ResAllocs;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Evaluator for resource allocation limits.
 */
class ResAllocsEvaluater {
    private final Map<String, ResAllocs> resAllocs;
    private final TaskTracker taskTracker;
    private final Set<String> failedTaskGroups = new HashSet<>();
    private final BlockingQueue<ResAllocs> addQ = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> remQ = new LinkedBlockingQueue<>();
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final List<ResAllocs> addList = new LinkedList<>();
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final List<String> remList = new LinkedList<>();

    ResAllocsEvaluater(TaskTracker taskTracker, Map<String, ResAllocs> initialResAllocs) {
        this.taskTracker = taskTracker;
        this.resAllocs = initialResAllocs==null? new HashMap<String, ResAllocs>() : new HashMap<>(initialResAllocs);
    }

    void replaceResAllocs(ResAllocs r) {
        if(r != null)
            addQ.offer(r);
    }

    void remResAllocs(String groupName) {
        if(groupName != null && !groupName.isEmpty())
            remQ.offer(groupName);
    }

    boolean prepare() {
        failedTaskGroups.clear();
        updateResAllocs();
        return !resAllocs.isEmpty();
    }

    private void updateResAllocs() {
        addQ.drainTo(addList);
        if(!addList.isEmpty()) {
            Iterator<ResAllocs> iter = addList.iterator();
            while(iter.hasNext()) {
                final ResAllocs r = iter.next();
                resAllocs.put(r.getTaskGroupName(), r);
                iter.remove();
            }
        }
        remQ.drainTo(remList);
        if(!remList.isEmpty()) {
            Iterator<String> iter = remList.iterator();
            while(iter.hasNext()) {
                resAllocs.remove(iter.next());
                iter.remove();
            }
        }
    }

    boolean taskGroupFailed(String taskGroupName) {
        return failedTaskGroups.contains(taskGroupName);
    }

    AssignmentFailure hasResAllocs(TaskRequest task) {
        if(resAllocs.isEmpty())
            return null;
        if(failedTaskGroups.contains(task.taskGroupName()))
            return new AssignmentFailure(VMResource.ResAllocs, 1.0, 0.0, 0.0, "");
        final TaskTracker.TaskGroupUsage usage = taskTracker.getUsage(task.taskGroupName());
        final ResAllocs resAllocs = this.resAllocs.get(task.taskGroupName());
        if(resAllocs == null)
            return new AssignmentFailure(VMResource.ResAllocs, 1.0, 0.0, 0.0, "");
        if(usage==null) {
            final boolean success = hasZeroUsageAllowance(resAllocs);
            if(!success)
                failedTaskGroups.add(task.taskGroupName());
            return success? null : new AssignmentFailure(VMResource.ResAllocs, 1.0, 0.0, 0.0, "");
        }
        // currently, no way to indicate which of resAllocs's resources limited us
        if((usage.getCores()+task.getCPUs()) > resAllocs.getCores())
            return new AssignmentFailure(VMResource.ResAllocs, task.getCPUs(), usage.getCores(), resAllocs.getCores(),
                    "");
        if((usage.getMemory() + task.getMemory()) > resAllocs.getMemory())
            return new AssignmentFailure(VMResource.ResAllocs, task.getMemory(), usage.getMemory(), resAllocs.getMemory(),
                    "");
        if((usage.getNetworkMbps()+task.getNetworkMbps()) > resAllocs.getNetworkMbps())
            return new AssignmentFailure(
                    VMResource.ResAllocs, task.getNetworkMbps(), usage.getNetworkMbps(), resAllocs.getNetworkMbps(), "");
        if((usage.getDisk()+task.getDisk()) > resAllocs.getDisk())
            return new AssignmentFailure(VMResource.ResAllocs, task.getDisk(), usage.getDisk(), resAllocs.getDisk(), "");
        return null;
    }

    private boolean hasZeroUsageAllowance(ResAllocs resAllocs) {
        return resAllocs != null &&
                (resAllocs.getCores() > 0.0 || resAllocs.getMemory() > 0.0 ||
                        resAllocs.getNetworkMbps() > 0.0 || resAllocs.getDisk() > 0.0);
    }

    public Map<String, ResAllocs> getResAllocs() {
        return Collections.unmodifiableMap(resAllocs);
    }

}
