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

import com.netflix.fenzo.functions.Action1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @warn class description missing
 */
class AssignableVMs {

    /**
     * @warn class description missing
     */
    static class VMRejectLimiter {
        private long lastRejectAt=0;
        private int rejectedCount;
        private final int limit;
        private final long rejectDelay;

        VMRejectLimiter(int limit, long leaseOfferExpirySecs) {
            this.limit = limit;
            this.rejectDelay = leaseOfferExpirySecs;
        }
        /**
         * @warn method description missing
         *
         * @return
         */
        synchronized boolean reject() {
            if(rejectedCount==limit)
                return false;
            rejectedCount++;
            lastRejectAt = System.currentTimeMillis();
            return true;
        }
        /**
         * @warn method description missing
         *
         * @return
         */
        boolean limitReached() {
            return rejectedCount == limit;
        }
        /**
         * @warn method description missing
         */
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
    private final ConcurrentMap<String, String> vmIdToHostnameMap = new ConcurrentHashMap<>();
    private final TaskTracker taskTracker;
    private final String attrNameToGroupMaxResources;
    private final Map<String, Map<VMResource, Double>> maxResourcesMap;
    private final VMRejectLimiter vmRejectLimiter;
    private final AssignableVirtualMachine dummyVM = new AssignableVirtualMachine(null, null, "", null, 0L, null) {
        @Override
        void assignResult(TaskAssignmentResult result) {
            throw new UnsupportedOperationException();
        }
    };
    private final ActiveVmGroups activeVmGroups;
    private String activeVmGroupAttributeName=null;
    private final List<String> unknownLeaseIdsToExpire = new ArrayList<>();

    AssignableVMs(TaskTracker taskTracker, Action1<VirtualMachineLease> leaseRejectAction, long leaseOfferExpirySecs, String attrNameToGroupMaxResources) {
        this.taskTracker = taskTracker;
        virtualMachinesMap = new ConcurrentHashMap<>();
        this.leaseRejectAction = leaseRejectAction;
        this.leaseOfferExpirySecs = leaseOfferExpirySecs;
        this.attrNameToGroupMaxResources = attrNameToGroupMaxResources;
        maxResourcesMap = new HashMap<>();
        vmRejectLimiter = new VMRejectLimiter(4, leaseOfferExpirySecs);  // ToDo make this configurable?
        activeVmGroups = new ActiveVmGroups();
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    Map<String, Map<VMResource, Double[]>> getResourceStatus() {
        Map<String, Map<VMResource, Double[]>> result = new HashMap<>();
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            result.put(avm.getHostname(), avm.getResourceStatus());
        return result;
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     *
     * @param request
     * @param host
     */
    void setTaskAssigned(TaskRequest request, String host) {
        createAvmIfAbsent(host);
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        avm.setAssignedTask(request);
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     *
     * @param taskId
     * @param host
     */
    void unAssignTask(String taskId, String host) {
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        if(avm != null) {
            avm.markTaskForUnassigning(taskId);
        }
        else
            logger.warn("No VM for host " + host + " to unassign task " + taskId);
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param leases
     * @return
     */
    int addLeases(List<VirtualMachineLease> leases) {
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
        avm.resetResources();
        int rejected=0;
        for(VirtualMachineLease l: leases) {
            String host = l.hostname();
            createAvmIfAbsent(host);
            if(!virtualMachinesMap.get(host).addLease(l))
                rejected++;
        }
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            avm.updateCurrTotalLease();
        return rejected;
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param hostname
     */
    private void createAvmIfAbsent(String hostname) {
        if(virtualMachinesMap.get(hostname) == null)
            virtualMachinesMap.putIfAbsent(hostname,
                    new AssignableVirtualMachine(vmIdToHostnameMap, leaseIdToHostnameMap, hostname,
                            leaseRejectAction, leaseOfferExpirySecs, taskTracker));
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param leaseId
     */
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

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param hostname
     */
    void expireAllLeases(String hostname) {
        final AssignableVirtualMachine avm = virtualMachinesMap.get(hostname);
        if(avm!=null)
            avm.expireAllLeases();
    }

    /**
     * @warn method description missing
     */
    void expireAllLeases() {
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            avm.expireAllLeases();
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     *
     * @param host
     * @param until
     */
    void disableUntil(String host, long until) {
        createAvmIfAbsent(host);
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        avm.setDisabledUntil(until);
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param host
     */
    void enableVM(String host) {
        AssignableVirtualMachine avm = virtualMachinesMap.get(host);
        if(avm != null)
            avm.enable();
        else
            logger.warn("Can't enable host " + host + ", no such host");
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param vmId
     * @return
     */
    String getHostnameFromVMId(String vmId) {
        return vmIdToHostnameMap.get(vmId);
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param attributeName
     */
    void setActiveVmGroupAttributeName(String attributeName) {
        this.activeVmGroupAttributeName = attributeName;
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param vmGroups
     */
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

    /**
     * @warn method description missing
     *
     * @return
     */
    List<AssignableVirtualMachine> prepareAndGetOrderedVMs() {
        expireAnyUnknownLeaseIds();
        removeExpiredLeases();
        List<AssignableVirtualMachine> vms = new ArrayList<>();
        taskTracker.clearAssignedTasks();
        vmRejectLimiter.reset();
        // ToDo make this parallel maybe?
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

    private void removeExpiredLeases() {
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            avm.removeExpiredLeases(!isInActiveVmGroup(avm));
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param idleResourcesList
     * @return
     */
    int removeLimitedLeases(List<VirtualMachineLease> idleResourcesList) {
        int rejected=0;
        List<VirtualMachineLease> randomized = new ArrayList<>(idleResourcesList);
        // randomize the list so we don't always reject leases of the same VM before hitting the reject limit
        Collections.shuffle(randomized);
        for(VirtualMachineLease lease: randomized) {
            if(vmRejectLimiter.limitReached())
                break;
            AssignableVirtualMachine avm = virtualMachinesMap.get(lease.hostname());
            rejected += avm.expireLimitedLeases(vmRejectLimiter);
        }
        return rejected;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    int getTotalNumVMs() {
        return virtualMachinesMap.size();
    }

    /**
     * @warn method description missing
     */
    /* package */ void purgeInactiveVMs() {
        for(String hostname: virtualMachinesMap.keySet()) {
            AssignableVirtualMachine avm = virtualMachinesMap.get(hostname);
            if(avm != null) {
                if(!avm.isActive()) {
                    virtualMachinesMap.remove(hostname, avm);
                    if(avm.getCurrVMId() != null)
                        vmIdToHostnameMap.remove(avm.getCurrVMId(), avm.getHostname());
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
                        case Network:
                            Double savedVal = savedMaxResources.get(r)==null? 0.0 : savedMaxResources.get(r);
                            savedMaxResources.put(r, Math.max(savedVal, maxResources.get(r)));
                    }
                }
            }
        }
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param attrValue
     * @return
     */
    Map<VMResource, Double> getMaxResources(String attrValue) {
        return maxResourcesMap.get(attrValue);
    }

    /**
     * @warn method description missing
     * @warn parameter descriptions missing
     *
     * @param attrValue
     * @param task
     * @return
     */
    AssignmentFailure getFailedMaxResource(String attrValue, TaskRequest task) {
        AssignmentFailure savedFailure = null;
        for(Map.Entry<String, Map<VMResource, Double>> entry: maxResourcesMap.entrySet()) {
            if(attrValue!=null && !attrValue.equals(entry.getKey()))
                continue;
            final Map<VMResource, Double> maxResources = entry.getValue();
            AssignmentFailure failure = null;
            for(VMResource res: VMResource.values()) {
                switch (res) {
                    case CPU:
                        if(maxResources.get(VMResource.CPU) < task.getCPUs()) {
                            failure = new AssignmentFailure(VMResource.CPU, task.getCPUs(), 0.0, maxResources.get(VMResource.CPU));
                        }
                        break;
                    case Memory:
                        if(maxResources.get(VMResource.Memory) < task.getMemory())
                            failure = new AssignmentFailure(VMResource.Memory, task.getMemory(), 0.0, maxResources.get(VMResource.Memory));
                        break;
                    case Disk:
                        if(maxResources.get(VMResource.Disk) < task.getDisk())
                            failure = new AssignmentFailure(VMResource.Disk, task.getDisk(), 0.0, maxResources.get(VMResource.Disk));
                        break;
                    case Ports:
                        if(maxResources.get(VMResource.Ports) < task.getPorts())
                            failure = new AssignmentFailure(VMResource.Ports, task.getPorts(), 0.0, maxResources.get(VMResource.Ports));
                        break;
                    case Network:
                        if(maxResources.get(VMResource.Network) < task.getNetworkMbps())
                            failure = new AssignmentFailure(VMResource.Network, task.getNetworkMbps(), 0.0, maxResources.get(VMResource.Network));
                        break;
                    case VirtualMachine:
                    case Fitness:
                    case ResAllocs:
                        break;
                    default:
                        logger.error("Unknown resource type: " + res);
                }
                if(failure!=null)
                    break;
            }
            if(failure == null)
                return null;
            savedFailure = failure;
        }
        return savedFailure;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    ActiveVmGroups getActiveVmGroups() {
        return activeVmGroups;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    List<VirtualMachineCurrentState> getVmCurrentStates() {
        List<VirtualMachineCurrentState> result = new ArrayList<>();
        for(AssignableVirtualMachine avm: virtualMachinesMap.values())
            result.add(avm.getVmCurrentState());
        return result;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    AssignableVirtualMachine getDummyVM() {
        return dummyVM;
    }
}
