package com.netflix.fenzo;


import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class AssignableVirtualMachine implements Comparable<AssignableVirtualMachine>{

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
            if(num+currUsedPorts > totalPorts)
                return false;
            return true;
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

    private final Map<String, VirtualMachineLease> leasesMap;
    private final BlockingQueue<String> workersToUnAssign;
    private final BlockingQueue<String> leasesToExpire;
    private final AtomicBoolean expireAllLeasesNow;
    private final Action1<VirtualMachineLease> leaseRejectAction;
    private final long leaseOfferExpirySecs;
    private final String hostname;
    // ToDo abstract out into VMResources
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
    private Map<String, Protos.Attribute> currAttributesMap = new HashMap<>();
    // previouslyAssignedTasksMap contains tasks on this slave before current scheduling iteration started. This is
    // available for optimization of scheduling assignments for such things as locality with other similar tasks, etc.
    private final Map<String, TaskRequest> previouslyAssignedTasksMap;
    // assignmentResults contains results of assignments on this slave from the current scheduling iteration; they
    // haven't been launched yet
    private final Map<TaskRequest, TaskAssignmentResult> assignmentResults;
    private static final Logger logger = LoggerFactory.getLogger(AssignableVirtualMachine.class);
    private final ConcurrentMap<String, String> leaseIdToHostnameMap;
    private final ConcurrentMap<String, String> slaveIdToHostnameMap;
    private String currSlaveId=null;
    private final TaskTracker taskTracker;
    private volatile long disabledUntil=0L;
    // This may have to be configurable, but, for now weight the job's soft constraints more than system wide fitness calculators
    private double softConstraintWeightPercentage=75.0;
    private String exclusiveTaskId =null;

    public AssignableVirtualMachine(ConcurrentMap<String, String> slaveIdToHostnameMap,
                                    ConcurrentMap<String, String> leaseIdToHostnameMap,
                                    String hostname, Action1<VirtualMachineLease> leaseRejectAction,
                                    long leaseOfferExpirySecs, TaskTracker taskTracker) {
        this.slaveIdToHostnameMap = slaveIdToHostnameMap;
        this.leaseIdToHostnameMap = leaseIdToHostnameMap;
        this.hostname = hostname;
        this.leaseRejectAction = leaseRejectAction==null?
                new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        logger.warn("No lease reject action registered to reject lease id " + lease.getId() +
                                " on host " + lease.hostname());
                    }
                } :
                leaseRejectAction;
        this.leaseOfferExpirySecs = leaseOfferExpirySecs;
        this.taskTracker = taskTracker;
        this.leasesMap = new HashMap<>();
        this.leasesToExpire = new LinkedBlockingQueue<>();
        expireAllLeasesNow = new AtomicBoolean(false);
        this.workersToUnAssign = new LinkedBlockingQueue<>();
        this.previouslyAssignedTasksMap = new HashMap<>();
        this.assignmentResults = new HashMap<>();
    }

    private void setAvailableResources() {
        resetResources(!leasesMap.isEmpty()); // clear attributes only if there is at least one offer, else keep attributes from before
        for(VirtualMachineLease l: leasesMap.values()) {
            currTotalCpus += l.cpuCores();
            currTotalMemory += l.memoryMB();
            currTotalNetworkMbps += l.networkMbps();
            currTotalDisk += l.diskMB();
            if(l.portRanges()!=null)
                currPortRanges.addRanges(l.portRanges());
            if(l.getAttributeMap()!=null)
                currAttributesMap.putAll(l.getAttributeMap());
        }
        currTotalLease = createTotaledLease();
    }

    private void resetResources(boolean clearAttributes) {
        currTotalCpus=0.0;
        currUsedCpus=0.0;
        currTotalMemory=0.0;
        currUsedMemory=0.0;
        currTotalNetworkMbps=0.0;
        currUsedNetworkMbps=0.0;
        currTotalDisk=0.0;
        currUsedDisk=0.0;
        currPortRanges.clear();
        if(clearAttributes)
            currAttributesMap.clear();
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
            public String getSlaveID() {
                return "NoSlaveID-InternalVMLease";
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
                return Collections.unmodifiableMap(currAttributesMap);
            }
        };
    }

    int removeExpiredLeases(AssignableVMs.SlaveRejectLimiter slaveRejectLimiter, boolean all) {
        int rejected=0;
        long now = System.currentTimeMillis();
        Set<String> leasesToExpireIds = new HashSet<>();
        leasesToExpire.drainTo(leasesToExpireIds);
        Iterator<Map.Entry<String,VirtualMachineLease>> iterator = leasesMap.entrySet().iterator();
        boolean expireAll = expireAllLeasesNow.getAndSet(false) || all;
        while(iterator.hasNext()) {
            VirtualMachineLease l = iterator.next().getValue();
            if(expireAll || leasesToExpireIds.contains(l.getId())) {
                leaseIdToHostnameMap.remove(l.getId());
                if(expireAll)
                    leaseRejectAction.call(l);
                iterator.remove();
            }
            else if(l.getOfferedTime() < (now - leaseOfferExpirySecs*1000) && slaveRejectLimiter.reject()) {
                leaseIdToHostnameMap.remove(l.getId());
                leaseRejectAction.call(l);
                iterator.remove();
                rejected++;
            }
        }
        return rejected;
    }

    String getCurrSlaveId() {
        return currSlaveId;
    }

    boolean addLease(VirtualMachineLease lease) {
        if(currSlaveId != lease.getSlaveID()) {
            currSlaveId = lease.getSlaveID();
            slaveIdToHostnameMap.put(lease.getSlaveID(), hostname);
        }
        if(System.currentTimeMillis()<disabledUntil) {
            leaseRejectAction.call(lease);
            return false;
        }
        if(leasesMap.get(lease.getId()) != null)
            throw new IllegalStateException("Attempt to add duplicate lease with id=" + lease.getId());
        if(leaseIdToHostnameMap.putIfAbsent(lease.getId(), hostname) != null)
            logger.warn("Unexpected to add a lease that already exists for host " + hostname + ", lease ID: " + lease.getId());
        leasesMap.put(lease.getId(), lease);
        return true;
    }

    void setDisabledUntil(long disabledUntil) {
        this.disabledUntil = disabledUntil;
        Iterator<Map.Entry<String, VirtualMachineLease>> entriesIterator = leasesMap.entrySet().iterator();
        while(entriesIterator.hasNext()) {
            Map.Entry<String, VirtualMachineLease> entry = entriesIterator.next();
            leaseIdToHostnameMap.remove(entry.getValue().getId());
            leaseRejectAction.call(entry.getValue());
            entriesIterator.remove();
        }
    }

    public void enable() {
        disabledUntil = 0;
    }

    long getDisabledUntil() {
        return disabledUntil;
    }

    /* package */ boolean isActive() {
        if( !leasesMap.isEmpty() ||
                hasPreviouslyAssignedTasks() ||
                !leasesToExpire.isEmpty() ||
                !workersToUnAssign.isEmpty() ||
                System.currentTimeMillis()<disabledUntil)
            return true;
        return false;
    }

    boolean isAssignableNow() {
        return (System.currentTimeMillis()>disabledUntil) && !leasesMap.isEmpty();
    }

    void setAssignedTask(TaskRequest request) {
        if(!taskTracker.addRunningTask(request, this))
            logger.error("Unexpected to add duplicate task id=" + request.getId());
        previouslyAssignedTasksMap.put(request.getId(), request);
        setIfExclusive(request);
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
        List<String> tasks = new ArrayList<>();
        workersToUnAssign.drainTo(tasks);
        for(String t: tasks) {
            taskTracker.removeRunningTask(t);
            previouslyAssignedTasksMap.remove(t);
            clearIfExclusive(t);
        }
        assignmentResults.clear();
        setAvailableResources();
    }

    String getAttrValue(String attrName) {
        Protos.Attribute attribute = getCurrTotalLease().getAttributeMap().get(attrName);
        if(attribute==null)
            return null;
        return attribute.getText().getValue();
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

    TaskAssignmentResult tryRequest(TaskRequest request, VMTaskFitnessCalculator fitnessCalculator) {
        //logger.info("    #leases=" + leases.size());
        if(leasesMap.isEmpty())
            return null;
        if(exclusiveTaskId!=null) {
            ConstraintFailure failure = new ConstraintFailure(ExclusiveHostConstraint.class.getName(),
                    "Already has task " + exclusiveTaskId + " with exclusive host constraint");
            return new TaskAssignmentResult(this, request, false, null, failure, 0.0);
        }
        VirtualMachineCurrentState vmCurrentState = vmCurrentState();
        TaskTrackerState taskTrackerState = taskTrackerState();
        ConstraintFailure failedHardConstraint = findFailedHardConstraints(request, vmCurrentState, taskTrackerState);
        if(failedHardConstraint!=null) {
            return new TaskAssignmentResult(this, request, false, null, failedHardConstraint, 0.0);
        }
        List<AssignmentFailure> failures = evalAndGetResourceAssignmentFailures(request);
        if(!failures.isEmpty()) {
            return new TaskAssignmentResult(this, request, false, failures, null, 0.0);
        }
        double fitness = fitnessCalculator.calculateFitness(request, vmCurrentState, taskTrackerState);
        if(fitness == 0.0) {
            failures.add(new AssignmentFailure(VMResource.Fitness, 1.0, 1.0, 0.0));
            return new TaskAssignmentResult(this, request, false, failures, null, fitness);
        }
        List<? extends VMTaskFitnessCalculator> softConstraints = request.getSoftConstraints();
        // we don't fail on soft constraints
        if(softConstraints!=null && !softConstraints.isEmpty()) {
            double softConstraintFitness = getSoftConstraintsFitness(request, vmCurrentState, taskTrackerState);
            fitness = ((softConstraintFitness*softConstraintWeightPercentage) + (fitness*(100.0-softConstraintWeightPercentage)))
                    / 100.0;
        }
        return new TaskAssignmentResult(this, request, true, null, null, fitness);
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

    private List<AssignmentFailure> evalAndGetResourceAssignmentFailures(TaskRequest request) {
        List<AssignmentFailure> failures = new ArrayList<>();
        if((currUsedCpus+request.getCPUs()) > currTotalCpus) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.CPU, request.getCPUs(), currUsedCpus,
                    currTotalCpus);
            logger.info(hostname+":"+request.getId()+" Insufficient cpus: " + failure.toString());
            failures.add(failure);
        }
        if((currUsedMemory+request.getMemory()) > currTotalMemory) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.Memory, request.getMemory(), currUsedMemory,
                    currTotalMemory);
            logger.info(hostname+":"+request.getId()+" Insufficient memory: " + failure.toString());
            failures.add(failure);
        }
        if((currUsedNetworkMbps+request.getNetworkMbps()) > currTotalNetworkMbps) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.Network, request.getNetworkMbps(), currUsedNetworkMbps, currTotalNetworkMbps);
            logger.info(hostname+":"+request.getId()+" Insufficient network: " + failure.toString());
            failures.add(failure);
        }
        if((currUsedDisk+request.getDisk()) > currTotalDisk) {
            AssignmentFailure failure = new AssignmentFailure(VMResource.Disk, request.getDisk(), currUsedDisk, currTotalDisk);
            logger.info(hostname+":"+request.getId()+" Insufficient disk: " + failure.toString());
            failures.add(failure);
        }
        if(!currPortRanges.hasPorts(request.getPorts())) {
            AssignmentFailure failure = new AssignmentFailure(
                    VMResource.Ports, request.getPorts(), currPortRanges.currUsedPorts,
                    currPortRanges.totalPorts);
            logger.info(hostname+":"+request.getId()+" Insufficient ports: " + failure.toString());
            failures.add(failure);
        }
        return failures;
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
        return new VirtualMachineCurrentState() {
            @Override
            public String getHostname() {
                return hostname;
            }
            @Override
            public VirtualMachineLease getCurrAvailableResources() {
                return currTotalLease;
            }
            @Override
            public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned() {
                return Collections.EMPTY_LIST;
            }
            @Override
            public Collection<TaskRequest> getRunningTasks() {
                return Collections.unmodifiableCollection(previouslyAssignedTasksMap.values());
            }
        };
    }

    private VirtualMachineCurrentState vmCurrentState() {
        return new VirtualMachineCurrentState() {
            @Override
            public String getHostname() {
                return hostname;
            }
            @Override
            public VirtualMachineLease getCurrAvailableResources() {
                return currTotalLease;
            }
            @Override
            public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned() {
                return Collections.unmodifiableCollection(assignmentResults.values());
            }
            @Override
            public Collection<TaskRequest> getRunningTasks() {
                return Collections.unmodifiableCollection(previouslyAssignedTasksMap.values());
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

    void assignResult(TaskAssignmentResult result) {
        currUsedCpus += result.getRequest().getCPUs();
        currUsedMemory += result.getRequest().getMemory();
        currUsedNetworkMbps += result.getRequest().getNetworkMbps();
        for(int p=0; p<result.getRequest().getPorts(); p++){
            result.addPort(currPortRanges.consumeNextPort());
        }
        if(!taskTracker.addAssignedTask(result.getRequest(), this))
            logger.error("Unexpected to re-add task to assigned state, id=" + result.getRequest().getId());
        assignmentResults.put(result.getRequest(), result);
    }

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
        for(String l: leasesMap.keySet())
            leaseIdToHostnameMap.remove(l);
        leasesMap.clear();
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

    Map<VMResource, Double[]> getResourceStatus() {
        Map<VMResource, Double[]> resourceMap = new HashMap<>();
        double cpusUsed=0.0;
        double memUsed=0.0;
        double portsUsed=0.0;
        for(TaskRequest r: previouslyAssignedTasksMap.values()) {
            cpusUsed += r.getCPUs();
            memUsed += r.getMemory();
            portsUsed += r.getPorts();
        }
        double cpusAvail=0.0;
        double memAvail=0.0;
        double portsAvail=0;
        for(VirtualMachineLease l: leasesMap.values()) {
            cpusAvail += l.cpuCores();
            memAvail += l.memoryMB();
            for(VirtualMachineLease.Range range: l.portRanges())
                portsAvail += range.getEnd()-range.getBeg();
        }
        resourceMap.put(VMResource.CPU, new Double[]{cpusUsed, cpusAvail});
        resourceMap.put(VMResource.Memory, new Double[]{memUsed, memAvail});
        resourceMap.put(VMResource.Ports, new Double[]{portsUsed, portsAvail});
        return resourceMap;
    }
}
