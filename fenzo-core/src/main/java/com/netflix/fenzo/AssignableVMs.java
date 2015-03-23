package com.netflix.fenzo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class AssignableVMs {

    static class SlaveRejectLimiter {
        private long lastRejectAt=0;
        private int rejectedCount;
        private final int limit;
        private final long rejectDelay;

        SlaveRejectLimiter(int limit, long leaseOfferExpirySecs) {
            this.limit = limit;
            rejectDelay = leaseOfferExpirySecs;
        }
        synchronized boolean reject() {
            if(rejectedCount==limit)
                return false;
            rejectedCount++;
            lastRejectAt = System.currentTimeMillis();
            return true;
        }
        private void reset() {
            if(System.currentTimeMillis() > (lastRejectAt + rejectDelay))
                rejectedCount=0;
        }
    }

    private final ConcurrentMap<String, AssignableVirtualMachine> virtualMachinesMap;
    private final Action1<VirtualMachineLease> leaseRejectAction;
    private final long leaseOfferExpirySecs;
    private static final Logger logger = LoggerFactory.getLogger(AssignableVMs.class);
    private final ConcurrentMap<String, String> leaseIdToHostnameMap = new ConcurrentHashMap<>();
    private final TaskTracker taskTracker = new TaskTracker();
    private final String attrNameToGroupMaxResources;
    private final Map<String, Map<VMResource, Double>> maxResourcesMap;
    private final SlaveRejectLimiter slaveRejectLimiter;
    private final AssignableVirtualMachine dummyVM = new AssignableVirtualMachine(null, "", null, 0L, null) {
        @Override
        void assignResult(TaskAssignmentResult result) {
            throw new UnsupportedOperationException();
        }
    };
    private final ActiveVmGroups activeVmGroups;
    private String activeVmGroupAttributeName=null;
    private final List<String> unknownLeaseIdsToExpire = new ArrayList<>();

    AssignableVMs(Action1<VirtualMachineLease> leaseRejectAction, long leaseOfferExpirySecs, String attrNameToGroupMaxResources) {
        virtualMachinesMap = new ConcurrentHashMap<>();
        this.leaseRejectAction = leaseRejectAction;
        this.leaseOfferExpirySecs = leaseOfferExpirySecs;
        this.attrNameToGroupMaxResources = attrNameToGroupMaxResources;
        maxResourcesMap = new HashMap<>();
        slaveRejectLimiter = new SlaveRejectLimiter(2, leaseOfferExpirySecs);  // ToDo make this configurable?
        activeVmGroups = new ActiveVmGroups();
    }

    Map<String, Map<VMResource, Double[]>> getResourceStatus() {
        Map<String, Map<VMResource, Double[]>> result = new HashMap<>();
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            result.put(avm.getHostname(), avm.getResourceStatus());
        return result;
    }

    void setTaskAssigned(TaskRequest request, String host) {
        if(virtualMachinesMap.get(host) == null)
            virtualMachinesMap.putIfAbsent(host,
                    new AssignableVirtualMachine(leaseIdToHostnameMap, host, leaseRejectAction, leaseOfferExpirySecs, taskTracker));
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        avm.setAssignedTask(request);
    }

    void unAssignTask(String taskId, String host) {
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        if(avm != null) {
            avm.markTaskForUnassigning(taskId);
        }
        else
            logger.warn("No VM for host " + host + " to unassign task " + taskId);
    }

    int addLeases(List<VirtualMachineLease> leases) {
        int rejected=0;
        for(VirtualMachineLease l: leases) {
            String host = l.hostname();
            if(virtualMachinesMap.get(host) == null)
                virtualMachinesMap.putIfAbsent(host,
                        new AssignableVirtualMachine(leaseIdToHostnameMap, host, leaseRejectAction, leaseOfferExpirySecs, taskTracker));
            if(!virtualMachinesMap.get(host).addLease(l))
                rejected++;
        }
        return rejected;
    }

    void expireLease(String leaseId) {
        final String hostname = leaseIdToHostnameMap.get(leaseId);
        if(hostname==null) {
            unknownLeaseIdsToExpire.add(leaseId);
            return;
        }
        internalExpireLease(hostname, leaseId);
    }

    private void internalExpireLease(String leaseId, String hostname) {
        AssignableVirtualMachine avm = virtualMachinesMap.get(hostname);
        if(avm != null)
            avm.expireLease(leaseId);
    }

    void expireAllLeases(String hostname) {
        final AssignableVirtualMachine avm = virtualMachinesMap.get(hostname);
        if(avm!=null)
            avm.expireAllLeases();
    }

    void expireAllLeases() {
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            avm.expireAllLeases();
    }

    void disableUntil(String host, long until) {
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        if(avm != null)
            avm.setDisabledUntil(until);
        else
            logger.warn("Can't disable host " + host + " until " + until + ", no such host");
    }

    void enableVM(String host) {
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        if(avm != null)
            avm.enable();
        else
            logger.warn("Can't enable host " + host + ", no such host");
    }

    void setActiveVmGroupAttributeName(String attributeName) {
        this.activeVmGroupAttributeName = attributeName;
    }

    void setActiveVmGroups(List<String> vmGroups) {
        activeVmGroups.setActiveVmGroups(vmGroups);
    }

    private boolean isInActiveVmGroup(AssignableVirtualMachine avm) {
        final String attrValue = avm.getAttrValue(activeVmGroupAttributeName);
        return activeVmGroups.isActiveVmGroup(attrValue, false);
    }

    private void expireAnyUnknownLeaseIds() {
        if(!unknownLeaseIdsToExpire.isEmpty()) {
            for(String leaseId: unknownLeaseIdsToExpire) {
                final String hostname = leaseIdToHostnameMap.get(leaseId);
                if(hostname!=null)
                    internalExpireLease(leaseId, hostname);
            }
            unknownLeaseIdsToExpire.clear();
        }
    }

    List<AssignableVirtualMachine> prepareAndGetOrderedVMs() {
        expireAnyUnknownLeaseIds();
        List<AssignableVirtualMachine> vms = new ArrayList<>();
        taskTracker.clearAssignedTasks();
        // ToDo make this parallel
        slaveRejectLimiter.reset();
        for(Map.Entry<String, AssignableVirtualMachine> entry: virtualMachinesMap.entrySet()) {
            AssignableVirtualMachine avm = entry.getValue();
            avm.prepareForScheduling();
            if(isInActiveVmGroup(entry.getValue()) && entry.getValue().isAssignableNow()) {
                // for now, only add it if it is available right now
                vms.add(avm);
            }
            saveMaxResources(avm);
        }
        //Collections.sort(vms);
        return vms;
    }

    int cleanup() {
        int rejected=0;
        List<AssignableVirtualMachine> randomized = new ArrayList<>(virtualMachinesMap.values());
        // randomize the list so we don't always reject leases of the same slave before hitting the reject limit
        Collections.shuffle(randomized);
        for(AssignableVirtualMachine avm: randomized) {
            rejected += avm.removeExpiredLeases(slaveRejectLimiter, !isInActiveVmGroup(avm));
        }
        return rejected;
    }

    int getTotalNumVMs() {
        return virtualMachinesMap.size();
    }

    /* package */ void purgeInactiveVMs() {
        for(String hostname: virtualMachinesMap.keySet()) {
            AssignableVirtualMachine avm = virtualMachinesMap.get(hostname);
            if(avm != null) {
                if(!avm.isActive()) {
                    virtualMachinesMap.remove(hostname, avm);
                    logger.info("Removed inactive host " + hostname);
                }
            }
        }
    }

    private void saveMaxResources(AssignableVirtualMachine avm) {
        if(attrNameToGroupMaxResources!=null && !attrNameToGroupMaxResources.isEmpty()) {
            String attrValue = avm.getAttrValue(attrNameToGroupMaxResources);
            if(attrValue !=null) {
                Map<VMResource, Double> maxResources = avm.getMaxResources();
                Map<VMResource, Double> savedMaxResources = maxResourcesMap.get(attrValue);
                if(savedMaxResources==null) {
                    savedMaxResources = new HashMap<>();
                    maxResourcesMap.put(attrValue, savedMaxResources);
                }
                for(VMResource r: VMResource.values()) {
                    switch (r) {
                        case CPU:
                        case Disk:
                        case Memory:
                        case Ports:
                            Double savedVal = savedMaxResources.get(r)==null? 0.0 : savedMaxResources.get(r);
                            savedMaxResources.put(r, Math.max(savedVal, maxResources.get(r)));
                    }
                }
            }
        }
    }

    Map<VMResource, Double> getMaxResources(String attrValue) {
        return maxResourcesMap.get(attrValue);
    }

    AssignmentFailure getFailedMaxResource(String attrValue, TaskRequest task) {
        for(Map.Entry<String, Map<VMResource, Double>> entry: maxResourcesMap.entrySet()) {
            if(attrValue!=null && !attrValue.equals(entry.getKey()))
                continue;
            final Map<VMResource, Double> maxResources = entry.getValue();
            for(VMResource res: VMResource.values()) {
                switch (res) {
                    case CPU:
                        if(maxResources.get(VMResource.CPU) < task.getCPUs()) {
                            return new AssignmentFailure(VMResource.CPU, task.getCPUs(), 0.0, maxResources.get(VMResource.CPU));
                        }
                        break;
                    case Memory:
                        if(maxResources.get(VMResource.Memory) < task.getMemory())
                            return new AssignmentFailure(VMResource.Memory, task.getMemory(), 0.0, maxResources.get(VMResource.Memory));
                        break;
                    case Disk:
                        if(maxResources.get(VMResource.Disk) < task.getDisk())
                            return new AssignmentFailure(VMResource.Disk, task.getDisk(), 0.0, maxResources.get(VMResource.Disk));
                        break;
                    case Ports:
                        if(maxResources.get(VMResource.Ports) < task.getPorts())
                            return new AssignmentFailure(VMResource.Ports, task.getPorts(), 0.0, maxResources.get(VMResource.Ports));
                        break;
                    case VirtualMachine:
                    case Fitness:
                        break;
                    default:
                        logger.error("Unknown resource type: " + res);
                }
            }
            return null; // at least one set of maxResources satisfies the task
        }
        return null;
    }

    ActiveVmGroups getActiveVmGroups() {
        return activeVmGroups;
    }

    List<VirtualMachineCurrentState> getVmCurrentStates() {
        List<VirtualMachineCurrentState> result = new ArrayList<>();
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            result.add(avm.getVmCurrentState());
        return result;
    }

    AssignableVirtualMachine getDummyVM() {
        return dummyVM;
    }
}
