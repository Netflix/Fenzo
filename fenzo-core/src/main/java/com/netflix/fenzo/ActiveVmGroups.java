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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Manage set of active VM groups.
 * A VM belongs to a VM group indicated by its value for the attribute name set via
 * {@link TaskScheduler#setActiveVmGroupAttributeName(String)}.
 */
class ActiveVmGroups {
    private static class VmGroup {
        private final long activatedAt;
        private final String name;

        private VmGroup(String name) {
            activatedAt = System.currentTimeMillis();
            this.name = name;
        }
        private long getActivatedAt() {
            return activatedAt;
        }
        private String getName() {
            return name;
        }
    }

    private final ConcurrentMap<Integer, List<VmGroup>> activeVmGroupsMap;
    private volatile long lastSetAt=0L;

    ActiveVmGroups() {
        activeVmGroupsMap = new ConcurrentHashMap<>();
        activeVmGroupsMap.put(0, new ArrayList<VmGroup>());
    }

    private VmGroup isIn(String vmg, List<VmGroup> list) {
        for(VmGroup g: list)
            if(g.getName().equals(vmg))
                return g;
        return null;
    }

    void setActiveVmGroups(List<String> vmGroups) {
        List<VmGroup> oldList = activeVmGroupsMap.get(0);
        List<VmGroup> vmGroupsList = new ArrayList<>();
        for(String vmg: vmGroups) {
            final VmGroup in = isIn(vmg, oldList);
            if(in == null)
                vmGroupsList.add(new VmGroup(vmg));
            else
                vmGroupsList.add(in);
        }
        lastSetAt = System.currentTimeMillis();
        activeVmGroupsMap.put(0, vmGroupsList);
    }

    long getLastSetAt() {
        return lastSetAt;
    }

    boolean isActiveVmGroup(String vmGroupName, boolean strict) {
        final List<VmGroup> vmGroupList = activeVmGroupsMap.get(0);
        if(vmGroupList.isEmpty())
            return true;
        if(strict && vmGroupName==null)
            return false;
        for(VmGroup group: vmGroupList) {
            if(group.getName().equals(vmGroupName))
                return true;
        }
        return false;
    }

    long getActivatedAt(String vmGroupName) {
        final List<VmGroup> vmGroupList = activeVmGroupsMap.get(0);
        for(VmGroup group: vmGroupList) {
            if(group.getName().equals(vmGroupName))
                return group.getActivatedAt();
        }
        return -1L;
    }
}
