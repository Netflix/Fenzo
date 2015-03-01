package io.mantisrx.fenzo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

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
