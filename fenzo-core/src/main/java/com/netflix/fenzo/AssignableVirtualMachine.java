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
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a VM that contains resources that can be assigned to tasks.
 */
class AssignableVirtualMachine implements Comparable<AssignableVirtualMachine>{

    /* package */ static final String PseuoHostNamePrefix = "FenzoPsueodHost-";

    private static class PortRange {
        private final VirtualMachineLease.Range range;
        private PortRange(VirtualMachineLease.Range range) {
            this.range = range;
        }
        int size() {
            return range.getEnd()-range.getBeg()+1;
        }
    }

    private static class PortRanges {
        private List<VirtualMachineLease.Range> ranges = new ArrayList<>();
        private List<PortRange> portRanges = new ArrayList<>();
        private int totalPorts=0;
        private int currUsedPorts=0;

        void addRanges(List<VirtualMachineLease.Range> ranges) {
            if(ranges!=null) {
                this.ranges.addAll(ranges);
                for(VirtualMachineLease.Range range: ranges) {
                    PortRange pRange = new PortRange(range);
                    portRanges.add(pRange);
                    totalPorts += pRange.size();
                }
            }
        }
        void clear() {
            ranges.clear();
            portRanges.clear();
            currUsedPorts=0;
            totalPorts=0;
        }
        private List<VirtualMachineLease.Range> getRanges() {
            return ranges;
        }
        boolean hasPorts(int num) {
            return num + currUsedPorts <= totalPorts;
        }
        private int consumeNextPort() {
            int forward=0;
            for(PortRange range: portRanges) {
                if(forward+range.size()>currUsedPorts) {
                    // consume in this range
                    return range.range.getBeg() + (currUsedPorts++ - forward);
                }
                else {
                    forward += range.size();
                }
            }
            throw new IllegalStateException("All ports (" + totalPorts + ") already used up");
        }
    }

    private static class ResAsgmntResult {
        private final List<AssignmentFailure> failures;
        private final double fitness;

        public ResAsgmntResult(List<AssignmentFailure> failures, double fitness) {
            this.failures = failures;
            this.fitness = fitness;
        }
    }

    private final PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator;
    private final Map<String, VirtualMachineLease> leasesMap;
    private final BlockingQueue<String> workersToUnAssign;
    private final BlockingQueue<String> leasesToExpire;
    private final AtomicBoolean expireAllLeasesNow;
    private final Action1<VirtualMachineLease> leaseRejectAction;
    private final long leaseOfferExpirySecs;
    private final String hostname;
    private final Map<String, Double> currTotalScalars = new HashMap<>();
    private final Map<String, Double> currUsedScalars = new HashMap<>();
    private double currTotalCpus=0.0;
    private double currUsedCpus=0.0;
    private double currTotalMemory=0.0;
    private double currUsedMemory=0.0;
    private double currTotalNetworkMbps=0.0;
    private double currUsedNetworkMbps=0.0;
    private double currTotalDisk=0.0;
    private double currUsedDisk=0.0;
    private VirtualMachineLease currTotalLease=null;
    private PortRanges currPortRanges = new PortRanges();
    private volatile Map<String, Protos.Attribute> currAttributesMap = Collections.emptyMap();
    private final Map<String, PreferentialNamedConsumableResourceSet> resourceSets = new HashMap<>();
    // previouslyAssignedTasksMap contains tasks on this VM before current scheduling iteration started. This is
    // available for optimization of scheduling assignments for such things as locality with other similar tasks, etc.
    private final Map<String, TaskRequest> previouslyAssignedTasksMap;
    // assignmentResults contains results of assignments on this VM from the current scheduling iteration; they
    // haven't been launched yet
    private final Map<TaskRequest, TaskAssignmentResult> assignmentResults;
    private static final Logger logger = LoggerFactory.getLogger(AssignableVirtualMachine.class);
    private final ConcurrentMap<String, String> leaseIdToHostnameMap;
    private final ConcurrentMap<String, String> vmIdToHostnameMap;
    private String currVMId =null;
    private final TaskTracker taskTracker;
    private volatile long disabledUntil=0L;
    // This may have to be configurable, but, for now weight the job's soft constraints more than system wide fitness calculators
    private static double softConstraintFitnessWeightPercentage =50.0;
    private static double rSetsFitnessWeightPercentage=15.0;
    private String exclusiveTaskId =null;
    private final boolean singleLeaseMode;
    private boolean firstLeaseAdded=false;
    private final List<TaskRequest> consumedResourcesToAssign = new ArrayList<>();

    public AssignableVirtualMachine(PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                                    ConcurrentMap<String, String> vmIdToHostnameMap,
                                    ConcurrentMap<String, String> leaseIdToHostnameMap,
                                    String hostname, Action1<VirtualMachineLease> leaseRejectAction,
                                    long leaseOfferExpirySecs, TaskTracker taskTracker) {
        this(preferentialNamedConsumableResourceEvaluator, vmIdToHostnameMap, leaseIdToHostnameMap, hostname, leaseRejectAction, leaseOfferExpirySecs, taskTracker, false);
    }

    public AssignableVirtualMachine(PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                                    ConcurrentMap<String, String> vmIdToHostnameMap,
                                    ConcurrentMap<String, String> leaseIdToHostnameMap,
                                    String hostname, Action1<VirtualMachineLease> leaseRejectAction,
                                    long leaseOfferExpirySecs, TaskTracker taskTracker, boolean singleLeaseMode) {
        this.preferentialNamedConsumableResourceEvaluator = preferentialNamedConsumableResourceEvaluator;
        this.vmIdToHostnameMap = vmIdToHostnameMap;
        this.leaseIdToHostnameMap = leaseIdToHostnameMap;
        this.hostname = hostname;
        this.leaseRejectAction = getWrappedLeaseRejectAction(leaseRejectAction);
        this.leaseOfferExpirySecs = leaseOfferExpirySecs;
        this.taskTracker = taskTracker;
        this.leasesMap = new HashMap<>();
        this.leasesToExpire = new LinkedBlockingQueue<>();
        expireAllLeasesNow = new AtomicBoolean(false);
        this.workersToUnAssign = new LinkedBlockingQueue<>();
        this.previouslyAssignedTasksMap = new HashMap<>();
        this.assignmentResults = new HashMap<>();
        this.singleLeaseMode = singleLeaseMode;
    }

    private Action1<VirtualMachineLease> getWrappedLeaseRejectAction(final Action1<VirtualMachineLease> leaseRejectAction) {
        return leaseRejectAction==null?
                lease -> logger.warn("No lease reject action registered to reject lease id " + lease.getId() +
                        " on host " + lease.hostname()) :
                lease -> {
                    if (isRejectable(lease))
                        leaseRejectAction.call(lease);
                };
    }

    private boolean isRejectable(VirtualMachineLease l) {
        return l != null && l.getOffer() != null;
    }

    private void addToAvailableResources(VirtualMachineLease l) {
        if(singleLeaseMode && firstLeaseAdded)
            return; // ToDo should this be illegal state exception?
        firstLeaseAdded = true;
        final Map<String, Double> scalars = l.getScalarValues();
        if(scalars != null && !scalars.isEmpty()) {
            for(Map.Entry<String, Double> entry: scalars.entrySet()) {
                Double currVal = currTotalScalars.get(entry.getKey());
                if(currVal == null)
                    currVal = 0.0;
                currTotalScalars.put(entry.getKey(), currVal + entry.getValue());
            }
        }
        currTotalCpus += l.cpuCores();
        currTotalMemory += l.memoryMB();
        currTotalNetworkMbps += l.networkMbps();
        currTotalDisk += l.diskMB();
        if (l.portRanges() != null)
            currPortRanges.addRanges(l.portRanges());
        if (l.getAttributeMap() != null) {
            // always replace attributes map with the latest
            currAttributesMap = Collections.unmodifiableMap(new HashMap<>(l.getAttributeMap()));
        }
        for(Map.Entry<String, Protos.Attribute> entry: currAttributesMap.entrySet()) {
            switch (entry.getKey()) {
                case "res":
                    String val = entry.getValue().getText().getValue();
                    if(val!=null) {
                        StringTokenizer tokenizer = new StringTokenizer(val, "-");
                        String resName = tokenizer.nextToken();
                        switch (resName) {
                            case PreferentialNamedConsumableResourceSet.attributeName:
                                if(tokenizer.countTokens() == 3) {
                                    String name = tokenizer.nextToken();
                                    String val0Str = tokenizer.nextToken();
                                    String val1Str = tokenizer.nextToken();
                                    if(!resourceSets.containsKey(name)) {
                                        try {
                                            int val0 = Integer.parseInt(val0Str);
                                            int val1 = Integer.parseInt(val1Str);
                                            final PreferentialNamedConsumableResourceSet crs =
                                                    new PreferentialNamedConsumableResourceSet(hostname, name, val0, val1);
                                            final Iterator<TaskRequest> iterator = consumedResourcesToAssign.iterator();
                                            while(iterator.hasNext()) {
                                                TaskRequest request = iterator.next();
                                                crs.assign(request);
                                                iterator.remove();
                                            }
                                            resourceSets.put(name, crs);
                                        }
                                        catch (NumberFormatException e) {
                                            logger.warn(hostname + ": invalid resource spec (" + val + ") in attributes, ignoring: " + e.getMessage());
                                        }
                                    }
                                }
                                else
                                    logger.warn("Invalid res spec (expected 4 tokens with delimiter '-', ignoring: " + val);
                                break;
                            default:
                                logger.warn("Unknown resource in attributes, ignoring: " + val);
                        }
                    }
                    break;
            }
        }
        if(!consumedResourcesToAssign.isEmpty()) {
            throw new IllegalStateException(hostname + ": Some assigned tasks have no resource sets in offers: " +
                    consumedResourcesToAssign);
        }
    }

    void updateCurrTotalLease() {
        currTotalLease = createTotaledLease();
    }

    void resetResources() {
        if(!singleLeaseMode) {
            currTotalCpus=0.0;
            currTotalMemory=0.0;
            currTotalNetworkMbps=0.0;
            currTotalDisk=0.0;
            currPortRanges.clear();
            currTotalScalars.clear();
        }
        currUsedCpus=0.0;
        currUsedMemory=0.0;
        currUsedNetworkMbps=0.0;
        currUsedDisk=0.0;
        currUsedScalars.clear();
        // ToDo: in single offer mode, need to resolve used ports somehow
        // don't clear attribute map
        for(VirtualMachineLease l: leasesMap.values())
            addToAvailableResources(l);
    }

    VirtualMachineLease getCurrTotalLease() {
        return currTotalLease;
    }

    private VirtualMachineLease createTotaledLease() {
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return "InternalVMLeaseObject";
            }
            @Override
            public long getOfferedTime() {
                return System.currentTimeMillis();
            }
            @Override
            public String hostname() {
                return hostname;
            }
            @Override
            public String getVMID() {
                return "NoVMID-InternalVMLease";
            }
            @Override
            public double cpuCores() {
                return currTotalCpus;
            }
            @Override
            public double memoryMB() {
                return currTotalMemory;
            }
            @Override
            public double networkMbps() {
                return currTotalNetworkMbps;
            }
            @Override
            public double diskMB() {
                return currTotalDisk;
            }
            @Override
            public List<Range> portRanges() {
                return Collections.unmodifiableList(currPortRanges.getRanges());
            }
            @Override
            public Protos.Offer getOffer() {
                return null;
            }
            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return currAttributesMap;
            }
            @Override
            public Double getScalarValue(String name) {
                return currTotalScalars.get(name);
            }
            @Override
            public Map<String, Double> getScalarValues() {
                return Collections.unmodifiableMap(currTotalScalars);
            }
        };
    }

    void removeExpiredLeases(boolean all) {
        removeExpiredLeases(all, true);
    }

    void removeExpiredLeases(boolean all, boolean doRejectCallback) {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") Set<String> leasesToExpireIds = new HashSet<>();
        leasesToExpire.drainTo(leasesToExpireIds);
        Iterator<Map.Entry<String,VirtualMachineLease>> iterator = leasesMap.entrySet().iterator();
        boolean expireAll = expireAllLeasesNow.getAndSet(false) || all;
        while(iterator.hasNext()) {
            VirtualMachineLease l = iterator.next().getValue();
            if(expireAll || leasesToExpireIds.contains(l.getId())) {
                leaseIdToHostnameMap.remove(l.getId());
                if(expireAll) {
                    if(logger.isDebugEnabled())
                        logger.debug(hostname + ": expiring lease offer id " + l.getId());
                    if (doRejectCallback)
                        leaseRejectAction.call(l);
                }
                iterator.remove();
                if (logger.isDebugEnabled())
                    logger.debug("Removed lease on {}, all={}", hostname, all);
            }
        }
        if(expireAll && !hasPreviouslyAssignedTasks())
            resourceSets.clear();
    }

    int expireLimitedLeases(AssignableVMs.VMRejectLimiter vmRejectLimiter) {
        if(singleLeaseMode)
            return 0;
        long now = System.currentTimeMillis();
        for (VirtualMachineLease l: leasesMap.values()) {
            if (isRejectable(l) && l.getOfferedTime() < (now - leaseOfferExpirySecs * 1000) && vmRejectLimiter.reject()) {
                for (VirtualMachineLease vml: leasesMap.values()) {
                    leaseIdToHostnameMap.remove(vml.getId());
                    if(logger.isDebugEnabled())
                        logger.debug(getHostname() + ": expiring lease offer id " + l.getId());
                    leaseRejectAction.call(vml);
                }
                int size = leasesMap.values().size();
                leasesMap.clear();
                if (logger.isDebugEnabled())
                    logger.debug(hostname + ": cleared leases");
                return size;
            }
        }
        return 0;
    }

    String getCurrVMId() {
        return currVMId;
    }

    boolean addLease(VirtualMachineLease lease) {
        if (logger.isDebugEnabled())
            logger.debug("{}: adding lease id {}", hostname, lease.getId());
        if(singleLeaseMode && firstLeaseAdded)
            return false;
        if(!Objects.equals(currVMId, lease.getVMID())) {
            currVMId = lease.getVMID();
            vmIdToHostnameMap.put(lease.getVMID(), hostname);
        }
        if(System.currentTimeMillis()<disabledUntil) {
            leaseRejectAction.call(lease);
            return false;
        }
        if (logger.isDebugEnabled())
            logger.debug(hostname + ": adding to internal leases map");
        if(leasesMap.get(lease.getId()) != null)
            throw new IllegalStateException("Attempt to add duplicate lease with id=" + lease.getId());
        if(leaseIdToHostnameMap.putIfAbsent(lease.getId(), hostname) != null)
            logger.warn("Unexpected to add a lease that already exists for host " + hostname + ", lease ID: " + lease.getId());
        if(logger.isDebugEnabled())
            logger.debug(getHostname() + ": adding lease offer id " + lease.getId());
        leasesMap.put(lease.getId(), lease);
        addToAvailableResources(lease);
        return true;
    }

    void setDisabledUntil(long disabledUntil) {
        this.disabledUntil = disabledUntil;
        if(logger.isDebugEnabled())
            logger.debug("{}: disabling for {} mSecs", hostname, (disabledUntil - System.currentTimeMillis()));
        Iterator<Map.Entry<String, VirtualMachineLease>> entriesIterator = leasesMap.entrySet().iterator();
        while(entriesIterator.hasNext()) {
            Map.Entry<String, VirtualMachineLease> entry = entriesIterator.next();
            leaseIdToHostnameMap.remove(entry.getValue().getId());
            leaseRejectAction.call(entry.getValue());
            entriesIterator.remove();
            if (logger.isDebugEnabled())
                logger.debug("Removed lease on " + hostname + " due to being disabled");
        }
    }

    public void enable() {
        disabledUntil = 0;
    }

    long getDisabledUntil() {
        return disabledUntil;
    }

    boolean isActive() {
        return !leasesMap.isEmpty() ||
                hasPreviouslyAssignedTasks() ||
                !assignmentResults.isEmpty() ||
                !leasesToExpire.isEmpty() ||
                !workersToUnAssign.isEmpty() ||
                System.currentTimeMillis() < disabledUntil;
    }

    boolean isAssignableNow() {
        return !isDisabled() && !leasesMap.isEmpty();
    }

    boolean isDisabled() {
        return System.currentTimeMillis() < disabledUntil;
    }

    void setAssignedTask(TaskRequest request) {
        if(logger.isDebugEnabled())
            logger.debug("{}: setting assigned task {}", hostname, request.getId());
        boolean added = taskTracker.addRunningTask(request, this);
        if(added) {
            assignResourceSets(request);
        }
        else
            logger.error("Unexpected to add duplicate task id=" + request.getId());
        previouslyAssignedTasksMap.put(request.getId(), request);
        setIfExclusive(request);
        if(singleLeaseMode && added) {
            removeResourcesOf(request);
        }
    }

    private void assignResourceSets(TaskRequest request) {
        if(request.getAssignedResources() != null) {
            final List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumedNamedResources =
                    request.getAssignedResources().getConsumedNamedResources();
            if(consumedNamedResources != null && !consumedNamedResources.isEmpty()) {
                for(PreferentialNamedConsumableResourceSet.ConsumeResult cr: consumedNamedResources) {
                    if(resourceSets.get(cr.getAttrName()) == null)
                        consumedResourcesToAssign.add(request); // resource set not available yet
                    else
                        resourceSets.get(cr.getAttrName()).assign(request);
                }
            }
        }
    }

    void expireLease(String leaseId) {
        logger.info("Got request to expire lease on " + hostname);
        leasesToExpire.offer(leaseId);
    }

    void expireAllLeases() {
        expireAllLeasesNow.set(true);
    }

    void markTaskForUnassigning(String taskId) {
        workersToUnAssign.offer(taskId);
    }

    private void setIfExclusive(TaskRequest request) {
        if(request.getHardConstraints()!=null) {
            for(ConstraintEvaluator evaluator: request.getHardConstraints()) {
                if(evaluator instanceof ExclusiveHostConstraint) {
                    exclusiveTaskId = request.getId();
                    return;
                }
            }
        }
    }

    private void clearIfExclusive(String taskId) {
        if(taskId.equals(exclusiveTaskId))
            exclusiveTaskId = null;
    }

    void prepareForScheduling() {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") List<String> tasks = new ArrayList<>();
        workersToUnAssign.drainTo(tasks);
        for(String t: tasks) {
            if(logger.isDebugEnabled())
                logger.debug("{}: removing previously assigned task {}", hostname, t);
            taskTracker.removeRunningTask(t);
            TaskRequest r = previouslyAssignedTasksMap.remove(t);
            if(singleLeaseMode && r!=null)
                addBackResourcesOf(r);
            releaseResourceSets(r);
            clearIfExclusive(t);
        }
        assignmentResults.clear();
    }

    private void releaseResourceSets(TaskRequest r) {
        if(r==null) {
            logger.warn("Can't release resource sets for null task");
            return;
        }
        // unassign resource sets if any
        final Map<String, TaskRequest.NamedResourceSetRequest> customNamedResources = r.getCustomNamedResources();
        for (Map.Entry<String, PreferentialNamedConsumableResourceSet> entry : resourceSets.entrySet()) {
            entry.getValue().release(r);
        }
    }

    private void removeResourcesOf(TaskRequest request) {
        currTotalCpus -= request.getCPUs();
        currTotalMemory -= request.getMemory();
        currTotalDisk -= request.getDisk();
        currTotalNetworkMbps -= request.getNetworkMbps();
        final Map<String, Double> scalarRequests = request.getScalarRequests();
        if(scalarRequests != null && !scalarRequests.isEmpty()) {
            for(Map.Entry<String, Double> entry: scalarRequests.entrySet()) {
                Double oldVal = currTotalScalars.get(entry.getKey());
                if(oldVal != null) {
                    double newVal = oldVal - entry.getValue();
                    if(newVal < 0.0) {
                        logger.warn(hostname + ": Scalar resource " + entry.getKey() + " is " + newVal + " after removing " +
                                entry.getValue() + " from task " + request.getId());
                        currTotalScalars.put(entry.getKey(), 0.0);
                    }
                    else
                        currTotalScalars.put(entry.getKey(), newVal);
                }
            }
        }
        // ToDo need to figure out ports as well
    }

    private void addBackResourcesOf(TaskRequest r) {
        currTotalCpus += r.getCPUs();
        currTotalMemory += r.getMemory();
        currTotalNetworkMbps += r.getNetworkMbps();
        currTotalDisk += r.getDisk();
        final Map<String, Double> scalarRequests = r.getScalarRequests();
        if(scalarRequests != null && !scalarRequests.isEmpty()) {
            for(Map.Entry<String, Double> entry: scalarRequests.entrySet()) {
                Double oldVal = currTotalScalars.get(entry.getKey());
                if(oldVal == null)
                    oldVal = 0.0;
                currTotalScalars.put(entry.getKey(), oldVal + entry.getValue());
            }
        }
        // ToDo queueTask back ports
    }

    String getAttrValue(String attrName) {
        if(getCurrTotalLease()==null)
            return null;
        Protos.Attribute attribute = getCurrTotalLease().getAttributeMap().get(attrName);
        if(attribute==null)
            return null;
        return attribute.getText().getValue();
    }

    Map<String, Double> getMaxScalars() {
        Map<String, Double> result = new HashMap<>();
        if (currTotalScalars != null) {
            for (Map.Entry<String, Double> e : currTotalScalars.entrySet()) {
                result.put(e.getKey(), e.getValue());
            }
        }
        if (hasPreviouslyAssignedTasks()) {
            for (TaskRequest t: previouslyAssignedTasksMap.values()) {
                final Map<String, Double> scalarRequests = t.getScalarRequests();
                if (scalarRequests != null && !scalarRequests.isEmpty()) {
                    for (Map.Entry<String, Double> e: scalarRequests.entrySet()) {
                        if (result.get(e.getKey()) == null)
                            result.put(e.getKey(), e.getValue());
                        else
                            result.put(e.getKey(), e.getValue() + result.get(e.getKey()));
                    }
                }
            }
        }
        return result;
    }

    Map<VMResource, Double> getMaxResources() {
        double cpus=0.0;
        double memory=0.0;
        double network=0.0;
        double ports=0.0;
        double disk=0.0;
        for(TaskRequest r: previouslyAssignedTasksMap.values()) {
            cpus += r.getCPUs();
            memory += r.getMemory();
            network += r.getNetworkMbps();
            ports += r.getPorts();
            disk += r.getDisk();
        }
        cpus += getCurrTotalLease().cpuCores();
        memory += getCurrTotalLease().memoryMB();
        network += getCurrTotalLease().networkMbps();
        List<VirtualMachineLease.Range> ranges = getCurrTotalLease().portRanges();
        for(VirtualMachineLease.Range r: ranges)
            ports += r.getEnd()-r.getBeg();
        disk += getCurrTotalLease().diskMB();
        Map<VMResource, Double> result = new HashMap<>();
        result.put(VMResource.CPU, cpus);
        result.put(VMResource.Memory, memory);
        result.put(VMResource.Network, network);
        result.put(VMResource.Ports, ports);
        result.put(VMResource.Disk, disk);
        return result;
    }

    /**
     * Try assigning resources for a given task.
     * This is the main allocation method to allocate resources from this VM to a given task. This method evaluates
     * hard constraints first. Then, it tries to assign resources. If either of these results in failures, it returns a
     * failure result. If successful, it invokes the fitness calculator to determine the fitness value. Then, it
     * evaluates soft constraints to get its fitness value. The resulting fitness value is reduced as a
     * weighted average of the two fitness values.
     *
     * @param request The task request to assign resources to.
     * @param fitnessCalculator The fitness calculator to use for resource assignment.
     * @return Assignment result.
     */
    TaskAssignmentResult tryRequest(TaskRequest request, VMTaskFitnessCalculator fitnessCalculator) {
        if(logger.isDebugEnabled())
            logger.debug("Host {} task {}: #leases: {}", getHostname(), request.getId(), leasesMap.size());
        if(leasesMap.isEmpty())
            return null;
        if(exclusiveTaskId!=null) {
            if(logger.isDebugEnabled())
                logger.debug("Host {}: can't assign task {}, already have task {} assigned with exclusive host constraint",
                        hostname, request.getId(), exclusiveTaskId);
            ConstraintFailure failure = new ConstraintFailure(ExclusiveHostConstraint.class.getName(),
                    "Already has task " + exclusiveTaskId + " with exclusive host constraint");
            return new TaskAssignmentResult(this, request, false, null, failure, 0.0);
        }
        VirtualMachineCurrentState vmCurrentState = vmCurrentState();
        TaskTrackerState taskTrackerState = taskTrackerState();
        ConstraintFailure failedHardConstraint = findFailedHardConstraints(request, vmCurrentState, taskTrackerState);
        if(failedHardConstraint!=null) {
            if(logger.isDebugEnabled())
                logger.debug("Host {}: task {} failed hard constraint: ", hostname, request.getId(), failedHardConstraint);
            return new TaskAssignmentResult(this, request, false, null, failedHardConstraint, 0.0);
        }
        final ResAsgmntResult resAsgmntResult = evalAndGetResourceAssignmentFailures(request);
        if(!resAsgmntResult.failures.isEmpty()) {
            if(logger.isDebugEnabled()) {
                StringBuilder b = new StringBuilder();
                for(AssignmentFailure f: resAsgmntResult.failures)
                    b.append(f.toString()).append(" ; ");
                logger.debug("{}: task {} failed assignment: {}", hostname, request.getId(), b.toString());
            }
            return new TaskAssignmentResult(this, request, false, resAsgmntResult.failures, null, 0.0);
        }
        final double resAsgmntFitness = resAsgmntResult.fitness;
        double fitness = fitnessCalculator.calculateFitness(request, vmCurrentState, taskTrackerState);
        if(fitness == 0.0) {
            if(logger.isDebugEnabled())
                logger.debug("{}: task {} fitness calculator returned 0.0", hostname, request.getId());
            List<AssignmentFailure> failures = Collections.singletonList(
                    new AssignmentFailure(VMResource.Fitness, 0.0, 0.0, 0.0, "fitnessCalculator: 0.0"));
            return new TaskAssignmentResult(this, request, false, failures, null, fitness);
        }
        List<? extends VMTaskFitnessCalculator> softConstraints = request.getSoftConstraints();
        // we don't fail on soft constraints
        double softConstraintFitness=1.0;
        if(softConstraints!=null && !softConstraints.isEmpty()) {
            softConstraintFitness = getSoftConstraintsFitness(request, vmCurrentState, taskTrackerState);
        }
        fitness = combineFitnessValues(resAsgmntFitness, fitness, softConstraintFitness);
        return new TaskAssignmentResult(this, request, true, null, null, fitness);
    }

    private double combineFitnessValues(double resAsgmntFitness, double fitness, double softConstraintFitness) {
        return ( resAsgmntFitness * rSetsFitnessWeightPercentage +
                softConstraintFitness * softConstraintFitnessWeightPercentage +
                fitness * (100.0 - rSetsFitnessWeightPercentage - softConstraintFitnessWeightPercentage) )
                / 100.0;
    }

    private double getSoftConstraintsFitness(TaskRequest request, VirtualMachineCurrentState vmCurrentState, TaskTrackerState taskTrackerState) {
        List<? extends VMTaskFitnessCalculator> softConstraints = request.getSoftConstraints();
        int n=0;
        double sum=0.0;
        for(VMTaskFitnessCalculator s: softConstraints) {
            n++;
            sum += s.calculateFitness(request, vmCurrentState, taskTrackerState);
        }
        return sum/n;
    }

    private ResAsgmntResult evalAndGetResourceAssignmentFailures(TaskRequest request) {
        List<AssignmentFailure> failures = new ArrayList<>();
        final Map<String, Double> scalarRequests = request.getScalarRequests();
        if(scalarRequests != null && !scalarRequests.isEmpty()) {
            for(Map.Entry<String, Double> entry: scalarRequests.entrySet()) {
                if(entry.getValue() == null)
                    continue;
                Double u = currUsedScalars.get(entry.getKey());
                if(u == null)  u = 0.0;
                Double t = currTotalScalars.get(entry.getKey());
                if(t == null)  t=0.0;
                if(u + entry.getValue() > t) {
                    failures.add(new AssignmentFailure(
                            VMResource.Other, entry.getValue(), u, t, entry.getKey()
                    ));
                }
            }
        }
        if((currUsedCpus+request.getCPUs()) > currTotalCpus) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.CPU, request.getCPUs(), currUsedCpus,
                    currTotalCpus, "");
            //logger.info(hostname+":"+request.getId()+" Insufficient cpus: " + failure.toString());
            failures.add(failure);
        }
        if((currUsedMemory+request.getMemory()) > currTotalMemory) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.Memory, request.getMemory(), currUsedMemory,
                    currTotalMemory, "");
            //logger.info(hostname+":"+request.getId()+" Insufficient memory: " + failure.toString());
            failures.add(failure);
        }
        if((currUsedNetworkMbps+request.getNetworkMbps()) > currTotalNetworkMbps) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.Network, request.getNetworkMbps(), currUsedNetworkMbps, currTotalNetworkMbps, "");
            //logger.info(hostname+":"+request.getId()+" Insufficient network: " + failure.toString());
            failures.add(failure);
        }
        if((currUsedDisk+request.getDisk()) > currTotalDisk) {
            AssignmentFailure failure =
                    new AssignmentFailure(VMResource.Disk, request.getDisk(), currUsedDisk, currTotalDisk, "");
            //logger.info(hostname+":"+request.getId()+" Insufficient disk: " + failure.toString());
            failures.add(failure);
        }
        if(!currPortRanges.hasPorts(request.getPorts())) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.Ports, request.getPorts(), currPortRanges.currUsedPorts,
                    currPortRanges.totalPorts, "");
            //logger.info(hostname+":"+request.getId()+" Insufficient ports: " + failure.toString());
            failures.add(failure);
        }
        double rSetFitness=0.0;
        int numRSets=0;
        final Set<String> requestedNamedResNames = new HashSet<>(request.getCustomNamedResources()==null? Collections.<String>emptySet() :
                request.getCustomNamedResources().keySet());
        if(failures.isEmpty()) {
            // perform resource set checks only if no other assignment failures so far
            for (Map.Entry<String, PreferentialNamedConsumableResourceSet> entry : resourceSets.entrySet()) {
                if (!requestedNamedResNames.isEmpty())
                    requestedNamedResNames.remove(entry.getKey());
                final double fitness = entry.getValue().getFitness(request, preferentialNamedConsumableResourceEvaluator);
                if (fitness == 0.0) {
                    AssignmentFailure failure = new AssignmentFailure(VMResource.ResourceSet, 0.0, 0.0, 0.0,
                            "ResourceSet " + entry.getValue().getName() + " unavailable"
                    );
                    failures.add(failure);
                } else {
                    rSetFitness += fitness;
                    numRSets++;
                }
            }
            if (!requestedNamedResNames.isEmpty()) {
                // task requested resourceSets that aren't available on this host
                AssignmentFailure failure = new AssignmentFailure(VMResource.ResourceSet, 0.0, 0.0, 0.0,
                        "UnavailableResourceSets: " + requestedNamedResNames
                );
                failures.add(failure);
            } else {
                if (!failures.isEmpty()) {
                    rSetFitness = 0.0;
                } else if (numRSets > 1)
                    rSetFitness /= numRSets;
            }
        }
        return new ResAsgmntResult(failures, rSetFitness);
    }

    private TaskTrackerState taskTrackerState() {
        return new TaskTrackerState() {
            @Override
            public Map<String, TaskTracker.ActiveTask> getAllRunningTasks() {
                return taskTracker.getAllRunningTasks();
            }

            @Override
            public Map<String, TaskTracker.ActiveTask> getAllCurrentlyAssignedTasks() {
                return taskTracker.getAllAssignedTasks();
            }
        };
    }

    VirtualMachineCurrentState getVmCurrentState() {
        final List<Protos.Offer> offers = new LinkedList<>();
        for (VirtualMachineLease l: leasesMap.values()) {
            offers.add(l.getOffer());
        }
        return new VirtualMachineCurrentState() {
            @Override
            public String getHostname() {
                return hostname;
            }
            @Override
            public Map<String, PreferentialNamedConsumableResourceSet> getResourceSets() {
                return resourceSets;
            }
            @Override
            public VirtualMachineLease getCurrAvailableResources() {
                return currTotalLease;
            }

            @Override
            public Collection<Protos.Offer> getAllCurrentOffers() {
                System.out.println("****************************** ");
                return offers;
            }

            @Override
            public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned() {
                return Collections.emptyList();
            }
            @Override
            public Collection<TaskRequest> getRunningTasks() {
                return Collections.unmodifiableCollection(previouslyAssignedTasksMap.values());
            }
            @Override
            public long getDisabledUntil() {
                return disabledUntil;
            }
        };
    }

    private VirtualMachineCurrentState vmCurrentState() {
        final List<Protos.Offer> offers = new LinkedList<>();
        for (VirtualMachineLease l: leasesMap.values()) {
            offers.add(l.getOffer());
        }
        return new VirtualMachineCurrentState() {
            @Override
            public String getHostname() {
                return hostname;
            }
            @Override
            public Map<String, PreferentialNamedConsumableResourceSet> getResourceSets() {
                return resourceSets;
            }
            @Override
            public VirtualMachineLease getCurrAvailableResources() {
                return currTotalLease;
            }

            @Override
            public Collection<Protos.Offer> getAllCurrentOffers() {
                return offers;
            }

            @Override
            public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned() {
                return Collections.unmodifiableCollection(assignmentResults.values());
            }
            @Override
            public Collection<TaskRequest> getRunningTasks() {
                return Collections.unmodifiableCollection(previouslyAssignedTasksMap.values());
            }
            @Override
            public long getDisabledUntil() {
                return disabledUntil;
            }
        };
    }

    private ConstraintFailure findFailedHardConstraints(TaskRequest request, VirtualMachineCurrentState vmCurrentState, TaskTrackerState taskTrackerState) {
        List<? extends ConstraintEvaluator> hardConstraints = request.getHardConstraints();
        if(hardConstraints==null || hardConstraints.isEmpty())
            return null;
        for(ConstraintEvaluator c: hardConstraints) {
            ConstraintEvaluator.Result r = c.evaluate(request, vmCurrentState, taskTrackerState);
            if(!r.isSuccessful())
                return new ConstraintFailure(c.getName(), r.getFailureReason());
        }
        return null;
    }

    String getHostname() {
        return hostname;
    }

    boolean hasPreviouslyAssignedTasks() {
        return !previouslyAssignedTasksMap.isEmpty();
    }

    /**
     * Assign the given result and update internal counters for used resources. Use this to assign an individual
     * assignment result within a scheduling iteration.
     *
     * @param result The assignment result to assign.
     */
    void assignResult(TaskAssignmentResult result) {
        final Map<String, Double> scalarRequests = result.getRequest().getScalarRequests();
        if(scalarRequests != null && !scalarRequests.isEmpty()) {
            for(Map.Entry<String, Double> entry: scalarRequests.entrySet()) {
                if(entry.getValue() == null)
                    continue;
                Double u = currUsedScalars.get(entry.getKey());
                if(u == null)  u = 0.0;
                currUsedScalars.put(entry.getKey(), u + entry.getValue());
            }
        }
        currUsedCpus += result.getRequest().getCPUs();
        currUsedMemory += result.getRequest().getMemory();
        currUsedNetworkMbps += result.getRequest().getNetworkMbps();
        currUsedDisk += result.getRequest().getDisk();
        for(int p=0; p<result.getRequest().getPorts(); p++){
            result.addPort(currPortRanges.consumeNextPort());
        }
        for(Map.Entry<String, PreferentialNamedConsumableResourceSet> entry: resourceSets.entrySet()) {
            result.addResourceSet(entry.getValue().consume(result.getRequest(), preferentialNamedConsumableResourceEvaluator));
        }
        if(!taskTracker.addAssignedTask(result.getRequest(), this))
            logger.error("Unexpected to re-add task to assigned state, id=" + result.getRequest().getId());
        assignmentResults.put(result.getRequest(), result);
    }

    /**
     * Reset the assignment results of current scheduling iteration and return the total assignment result for this VM.
     * Use this at the end of the scheduling iteration. Include all of the assignment results as well as all of the VM
     * leases available in the result.
     *
     * @return Total assignment result including the tasks assigned and VM leases used.
     */
    VMAssignmentResult resetAndGetSuccessfullyAssignedRequests() {
        if(assignmentResults.isEmpty())
            return null;
        Set<TaskAssignmentResult> result = new HashSet<>();
        for(Map.Entry<TaskRequest, TaskAssignmentResult> entry: assignmentResults.entrySet())
            if(entry.getValue().isSuccessful())
                result.add(entry.getValue());
        if(result.isEmpty())
            return null;
        VMAssignmentResult vmar = new VMAssignmentResult(hostname, new ArrayList<>(leasesMap.values()), result);
        if(!singleLeaseMode) {
            for(String l: leasesMap.keySet())
                leaseIdToHostnameMap.remove(l);
            leasesMap.clear();
        }
        assignmentResults.clear();
        return vmar;
    }

    // Only makes sense to get called after leases have been consolidated and total resources set in
    // {@Code setAvailableResources()}
    @Override
    public int compareTo(AssignableVirtualMachine o) {
        if(o == null)
            return -1;
        if(o.leasesMap.isEmpty())
            return -1;
        if(leasesMap.isEmpty())
            return 1;
        return Double.compare(o.currTotalCpus, currTotalCpus);
    }

    /**
     * Get resource status, showing used and available amounts. The available amounts are in addition to the amounts used.
     *
     * @return Map with keys containing resources and values containing corresponding usage represented as a two number
     * array, where the first represents the used amounts and the second represents additional available amounts.
     */
    Map<VMResource, Double[]> getResourceStatus() {
        Map<VMResource, Double[]> resourceMap = new HashMap<>();
        double cpusUsed=0.0;
        double memUsed=0.0;
        double portsUsed=0.0;
        double networkUsed=0.0;
        double diskUsed=0.0;
        for(TaskRequest r: previouslyAssignedTasksMap.values()) {
            cpusUsed += r.getCPUs();
            memUsed += r.getMemory();
            portsUsed += r.getPorts();
            networkUsed += r.getNetworkMbps();
            diskUsed += r.getDisk();
        }
        double cpusAvail=0.0;
        double memAvail=0.0;
        double portsAvail=0;
        double networkAvail=0.0;
        double diskAvail=0.0;
        for(VirtualMachineLease l: leasesMap.values()) {
            cpusAvail += l.cpuCores();
            memAvail += l.memoryMB();
            for(VirtualMachineLease.Range range: l.portRanges())
                portsAvail += range.getEnd()-range.getBeg();
            networkAvail += l.networkMbps();
            diskAvail += l.diskMB();
        }
        resourceMap.put(VMResource.CPU, new Double[]{cpusUsed, cpusAvail});
        resourceMap.put(VMResource.Memory, new Double[]{memUsed, memAvail});
        resourceMap.put(VMResource.Ports, new Double[]{portsUsed, portsAvail});
        resourceMap.put(VMResource.Network, new Double[]{networkUsed, networkAvail});
        resourceMap.put(VMResource.Disk, new Double[]{diskUsed, diskAvail});
        // put resource sets
        for(PreferentialNamedConsumableResourceSet rSet: resourceSets.values()) {
            final String name = rSet.getName();
            final List<Double> usedCounts = rSet.getUsedCounts();
            int used=0;
            for(Double c: usedCounts) {
                if(c>=0)
                    used++;
            }
            resourceMap.put(VMResource.ResourceSet, new Double[]{(double)used, (double)(usedCounts.size()-used)});
        }
        return resourceMap;
    }
}
