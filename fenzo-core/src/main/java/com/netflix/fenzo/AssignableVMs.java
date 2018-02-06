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
import com.netflix.fenzo.functions.Func1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class AssignableVMs {

    static class VMRejectLimiter {
        private long lastRejectAt=0;
        private int rejectedCount;
        private final int limit;
        private final long rejectDelay;

        VMRejectLimiter(int limit, long leaseOfferExpirySecs) {
            this.limit = limit;
            this.rejectDelay = leaseOfferExpirySecs*1000L;
        }
        synchronized boolean reject() {
            if(rejectedCount==limit)
                return false;
            rejectedCount++;
            lastRejectAt = System.currentTimeMillis();
            return true;
        }
        boolean limitReached() {
            return rejectedCount == limit;
        }
        private void reset() {
            if(System.currentTimeMillis() > (lastRejectAt + rejectDelay))
                rejectedCount=0;
        }
    }

    private static class HostDisablePair {
        private final String host;
        private final Long until;

        HostDisablePair(String host, Long until) {
            this.host = host;
            this.until = until;
        }
    }

    private final VMCollection vmCollection;
    private static final Logger logger = LoggerFactory.getLogger(AssignableVMs.class);
    private final ConcurrentMap<String, String> leaseIdToHostnameMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> vmIdToHostnameMap = new ConcurrentHashMap<>();
    private final BlockingQueue<HostDisablePair> disableRequests = new LinkedBlockingQueue<>();
    private final TaskTracker taskTracker;
    private final String attrNameToGroupMaxResources;
    private final Map<String, Map<VMResource, Double>> maxResourcesMap;
    private final Map<VMResource, Double> totalResourcesMap;
    private final VMRejectLimiter vmRejectLimiter;
    private final AssignableVirtualMachine dummyVM = new AssignableVirtualMachine(null, null, null, "", null, 0L, null) {
        @Override
        void assignResult(TaskAssignmentResult result) {
            throw new UnsupportedOperationException();
        }
    };
    private final ActiveVmGroups activeVmGroups;
    private String activeVmGroupAttributeName=null;
    private final BlockingQueue<String> unknownLeaseIdsToExpire = new LinkedBlockingQueue<>();

    AssignableVMs(TaskTracker taskTracker, Action1<VirtualMachineLease> leaseRejectAction,
                  PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                  long leaseOfferExpirySecs, int maxOffersToReject,
                  String attrNameToGroupMaxResources, boolean singleLeaseMode, String autoScaleByAttributeName) {
        this.taskTracker = taskTracker;
        vmCollection = new VMCollection(
                hostname -> new AssignableVirtualMachine(preferentialNamedConsumableResourceEvaluator, vmIdToHostnameMap, leaseIdToHostnameMap, hostname,
                        leaseRejectAction, leaseOfferExpirySecs, taskTracker, singleLeaseMode),
                autoScaleByAttributeName
        );
        this.attrNameToGroupMaxResources = attrNameToGroupMaxResources;
        maxResourcesMap = new HashMap<>();
        totalResourcesMap = new HashMap<>();
        vmRejectLimiter = new VMRejectLimiter(maxOffersToReject, leaseOfferExpirySecs);  // ToDo make this configurable?
        activeVmGroups = new ActiveVmGroups();
    }

    VMCollection getVmCollection() {
        return vmCollection;
    }

    Map<String, List<String>> createPseudoHosts(Map<String, Integer> groupCounts, Func1<String, AutoScaleRule> ruleGetter) {
        return vmCollection.clonePseudoVMsForGroups(groupCounts, ruleGetter, lease ->
            lease != null &&
                    (lease.getAttributeMap() == null ||
                            lease.getAttributeMap().get(activeVmGroupAttributeName) == null ||
                            isInActiveVmGroup(lease.getAttributeMap().get(activeVmGroupAttributeName).getText().getValue())
                    )
        );
    }

    void removePseudoHosts(Map<String, List<String>> hostsMap) {
        if (hostsMap != null && !hostsMap.isEmpty()) {
            for (Map.Entry<String, List<String>> entry: hostsMap.entrySet()) {
                for (String h: entry.getValue()) {
                    final AssignableVirtualMachine avm = vmCollection.unsafeRemoveVm(h, entry.getKey());
                    if (avm != null)
                        avm.removeExpiredLeases(true, false);
                }
            }
        }
    }

    Map<String, Map<VMResource, Double[]>> getResourceStatus() {
        Map<String, Map<VMResource, Double[]>> result = new HashMap<>();
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs())
            result.put(avm.getHostname(), avm.getResourceStatus());
        return result;
    }

    void setTaskAssigned(TaskRequest request, String host) {
        vmCollection.getOrCreate(host).setAssignedTask(request);
    }

    void unAssignTask(String taskId, String host) {
        final Optional<AssignableVirtualMachine> vmByName = vmCollection.getVmByName(host);
        if(vmByName.isPresent()) {
            vmByName.get().markTaskForUnassigning(taskId);
        }
        else
            logger.warn("No VM for host " + host + " to unassign task " + taskId);
    }

    private int addLeases(List<VirtualMachineLease> leases) {
        if(logger.isDebugEnabled())
            logger.debug("Adding leases");
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs())
            avm.resetResources();
        int rejected=0;
        for(VirtualMachineLease l: leases) {
            if(vmCollection.addLease(l))
                rejected++;
        }
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs()) {
            if(logger.isDebugEnabled())
                logger.debug("Updating total lease on " + avm.getHostname());
            avm.updateCurrTotalLease();
            final VirtualMachineLease currTotalLease = avm.getCurrTotalLease();
            if(logger.isDebugEnabled()) {
                if (currTotalLease == null)
                    logger.debug("Updated total lease is null for " + avm.getHostname());
                else {
                    logger.debug("Updated total lease for {} has cpu={}, mem={}, disk={}, network={}",
                            avm.getHostname(), currTotalLease.cpuCores(), currTotalLease.memoryMB(),
                            currTotalLease.diskMB(), currTotalLease.networkMbps()
                    );
                }
            }
        }
        return rejected;
    }

    void expireLease(String leaseId) {
        final String hostname = leaseIdToHostnameMap.get(leaseId);
        if(hostname==null) {
            logger.debug("Received expiry request for an unknown lease: {}", leaseId);
            unknownLeaseIdsToExpire.offer(leaseId);
            return;
        }
        internalExpireLease(leaseId, hostname);
    }

    private void internalExpireLease(String leaseId, String hostname) {
        final Optional<AssignableVirtualMachine> vmByName = vmCollection.getVmByName(hostname);
        if(vmByName.isPresent()) {
            if(logger.isDebugEnabled())
                logger.debug("Expiring lease offer id " + leaseId + " on host " + hostname);
            vmByName.get().expireLease(leaseId);
        }
    }

    void expireAllLeases(String hostname) {
        final Optional<AssignableVirtualMachine> vmByName = vmCollection.getVmByName(hostname);
        if(vmByName.isPresent())
            vmByName.get().expireAllLeases();
    }

    void expireAllLeases() {
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs())
            avm.expireAllLeases();
    }

    void disableUntil(String host, long until) {
        disableRequests.offer(new HostDisablePair(host, until));
    }

    private void disableVMs() {
        if (disableRequests.peek() == null)
            return;
        List<HostDisablePair> disablePairs = new LinkedList<>();
        disableRequests.drainTo(disablePairs);
        for (HostDisablePair hostDisablePair: disablePairs) {
            final Optional<AssignableVirtualMachine> vmByName = vmCollection.getVmByName(hostDisablePair.host);
            if (vmByName.isPresent())
                vmByName.get().setDisabledUntil(hostDisablePair.until);
        }
    }

    void enableVM(String host) {
        final Optional<AssignableVirtualMachine> vmByName = vmCollection.getVmByName(host);
        if(vmByName.isPresent())
            vmByName.get().enable();
        else
            logger.warn("Can't enable host " + host + ", no such host");
    }

    String getHostnameFromVMId(String vmId) {
        return vmIdToHostnameMap.get(vmId);
    }

    void setActiveVmGroupAttributeName(String attributeName) {
        this.activeVmGroupAttributeName = attributeName;
    }

    void setActiveVmGroups(List<String> vmGroups) {
        activeVmGroups.setActiveVmGroups(vmGroups);
    }

    private boolean isInActiveVmGroup(AssignableVirtualMachine avm) {
        final String attrValue = avm.getAttrValue(activeVmGroupAttributeName);
        return isInActiveVmGroup(attrValue);
    }

    private boolean isInActiveVmGroup(String attrValue) {
        return activeVmGroups.isActiveVmGroup(attrValue, false);
    }

    private void expireAnyUnknownLeaseIds() {
        List<String> unknownExpiredLeases = new ArrayList<>();
        unknownLeaseIdsToExpire.drainTo(unknownExpiredLeases);
        for(String leaseId: unknownExpiredLeases) {
            final String hostname = leaseIdToHostnameMap.get(leaseId);
            if(hostname!=null)
                internalExpireLease(leaseId, hostname);
        }
    }

    List<AssignableVirtualMachine> prepareAndGetOrderedVMs(List<VirtualMachineLease> newLeases, AtomicInteger rejectedCount) {
        disableVMs();
        removeExpiredLeases();
        rejectedCount.addAndGet(addLeases(newLeases));
        expireAnyUnknownLeaseIds();
        List<AssignableVirtualMachine> vms = new ArrayList<>();
        taskTracker.clearAssignedTasks();
        vmRejectLimiter.reset();
        resetTotalResources();
        // ToDo make this parallel maybe?
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs()) {
            avm.prepareForScheduling();
            if(isInActiveVmGroup(avm) && avm.isAssignableNow()) {
                // for now, only add it if it is available right now
                if(logger.isDebugEnabled())
                    logger.debug("Host " + avm.getHostname() + " available for assignments");
                vms.add(avm);
            }
            else if(logger.isDebugEnabled())
                logger.debug("Host " + avm.getHostname() + " not available for assignments");
            saveMaxResources(avm);
            if (isInActiveVmGroup(avm) && !avm.isDisabled())
                addTotalResources(avm);
        }
        taskTracker.setTotalResources(totalResourcesMap);
        //Collections.sort(vms);
        return vms;
    }

    List<AssignableVirtualMachine> getInactiveVMs() {
        return vmCollection.getAllVMs().stream().filter(avm -> !isInActiveVmGroup(avm)).collect(Collectors.toList());
    }

    private void resetTotalResources() {
        totalResourcesMap.clear();
    }

    private void addTotalResources(AssignableVirtualMachine avm) {
        final Map<VMResource, Double> maxResources = avm.getMaxResources();
        for (VMResource r: maxResources.keySet()) {
            Double v = maxResources.get(r);
            if (v != null) {
                if (totalResourcesMap.get(r) == null)
                    totalResourcesMap.put(r, v);
                else
                    totalResourcesMap.put(r, totalResourcesMap.get(r) + v);
            }
        }
    }

    private void removeExpiredLeases() {
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs())
            avm.removeExpiredLeases(!isInActiveVmGroup(avm));
    }

    int removeLimitedLeases(List<VirtualMachineLease> idleResourcesList) {
        int rejected=0;
        List<VirtualMachineLease> randomized = new ArrayList<>(idleResourcesList);
        // randomize the list so we don't always reject leases of the same VM before hitting the reject limit
        Collections.shuffle(randomized);
        for(VirtualMachineLease lease: randomized) {
            if(vmRejectLimiter.limitReached())
                break;
            final Optional<AssignableVirtualMachine> vmByName = vmCollection.getVmByName(lease.hostname());
            if (vmByName.isPresent())
                rejected += vmByName.get().expireLimitedLeases(vmRejectLimiter);
        }
        return rejected;
    }

    int getTotalNumVMs() {
        return vmCollection.size();
    }

    void purgeInactiveVMs(Set<String> excludeVms) {
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs()) {
            if(avm != null) {
                if (!excludeVms.contains(avm.getHostname())) {
                    if (!avm.isActive()) {
                        vmCollection.remove(avm);
                        if (avm.getCurrVMId() != null)
                            vmIdToHostnameMap.remove(avm.getCurrVMId(), avm.getHostname());
                        logger.info("Removed inactive host " + avm.getHostname());
                    }
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

    Map<VMResource, Double> getMaxResources(String attrValue) {
        return maxResourcesMap.get(attrValue);
    }

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
                            failure = new AssignmentFailure(
                                    VMResource.CPU, task.getCPUs(), 0.0, maxResources.get(VMResource.CPU), "");
                        }
                        break;
                    case Memory:
                        if(maxResources.get(VMResource.Memory) < task.getMemory())
                            failure = new AssignmentFailure(
                                    VMResource.Memory, task.getMemory(), 0.0, maxResources.get(VMResource.Memory), "");
                        break;
                    case Disk:
                        if(maxResources.get(VMResource.Disk) < task.getDisk())
                            failure = new AssignmentFailure(
                                    VMResource.Disk, task.getDisk(), 0.0, maxResources.get(VMResource.Disk), "");
                        break;
                    case Ports:
                        if(maxResources.get(VMResource.Ports) < task.getPorts())
                            failure = new AssignmentFailure(
                                    VMResource.Ports, task.getPorts(), 0.0, maxResources.get(VMResource.Ports), "");
                        break;
                    case Network:
                        if(maxResources.get(VMResource.Network) < task.getNetworkMbps())
                            failure = new AssignmentFailure(
                                    VMResource.Network, task.getNetworkMbps(), 0.0, maxResources.get(VMResource.Network), "");
                        break;
                    case VirtualMachine:
                    case Fitness:
                    case ResAllocs:
                    case ResourceSet:
                    case Other:
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

    ActiveVmGroups getActiveVmGroups() {
        return activeVmGroups;
    }

    List<VirtualMachineCurrentState> getVmCurrentStates() {
        List<VirtualMachineCurrentState> result = new ArrayList<>();
        for(AssignableVirtualMachine avm: vmCollection.getAllVMs())
            result.add(avm.getVmCurrentState());
        return result;
    }

    AssignableVirtualMachine getDummyVM() {
        return dummyVM;
    }
}
