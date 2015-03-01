package io.mantisrx.fenzo;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class BasicSchedulerTests {
    private TaskScheduler taskScheduler;
    @Before
    public void setUp() throws Exception {
        taskScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                    }
                })
                .build();
    }

    @After
    public void tearDown() throws Exception {

    }

    // verify that we're using all resources on one lease
    @Test
    public void testScheduler1() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        String host1 = leases.get(0).hostname();
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(2, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        Assert.assertEquals(host1, resultMap.keySet().iterator().next());
        Assert.assertEquals(host1, resultMap.values().iterator().next().getHostname());
        Assert.assertEquals(1, resultMap.values().iterator().next().getLeasesUsed().size());
        Assert.assertEquals(taskRequests.size(), resultMap.values().iterator().next().getTasksAssigned().size());
    }

    @Test
    public void testInsufficientCPUs() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(5, 10, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.entrySet().size());
    }

    @Test
    public void testInsufficientCPUs2() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(2, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(2, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        Assert.assertEquals(2, resultMap.values().iterator().next().getTasksAssigned().size());
    }

    @Test
    public void testInsufficientMemory() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 95, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(2, 95, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 95, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        Assert.assertEquals(1, resultMap.values().iterator().next().getTasksAssigned().size());
    }

    @Test
    public void testInsufficientPorts() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 6));
        taskRequests.add(TaskRequestProvider.getTaskRequest(2, 10, 6));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 6));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        Assert.assertEquals(1, resultMap.values().iterator().next().getTasksAssigned().size());
    }

    private boolean atLeastOneInRange(List<Integer> check, int beg, int end) {
        for(Integer c: check)
            if(c>=beg && c<=end)
                return true;
        return false;
    }

    @Test
    public void testPortsUsedAcrossRanges() throws Exception {
        VirtualMachineLease.Range range1 = new VirtualMachineLease.Range(1, 4);
        VirtualMachineLease.Range range2 = new VirtualMachineLease.Range(5, 10);
        List<VirtualMachineLease.Range> ranges = new ArrayList<>(2);
        ranges.add(range1);
        ranges.add(range2);
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, ranges);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 6));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        Assert.assertEquals(1, resultMap.values().iterator().next().getTasksAssigned().size());
        TaskAssignmentResult result = resultMap.values().iterator().next().getTasksAssigned().iterator().next();
        List<Integer> ports = result.getAssignedPorts();
        Assert.assertEquals(6, ports.size());
        Assert.assertEquals(true, atLeastOneInRange(ports, range1.getBeg(), range1.getEnd()));
        Assert.assertEquals(true, atLeastOneInRange(ports, range2.getBeg(), range2.getEnd()));
    }

    @Test
    public void testRepeatedPortsUsage() throws Exception {
        // verify that all ports of a machine can get used with repeated allocation, i.e., there is no ports leak
        double memPerJob=2;
        int portBeg=1;
        int portEnd=10;
        int numPortsPerJob=2;
        int numJobs = (portEnd-portBeg+1)/numPortsPerJob;
        List<VirtualMachineLease> leases = new ArrayList<>();
        List<TaskRequest> taskRequests = new ArrayList<>();
        VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", numJobs, numJobs*memPerJob, portBeg, portEnd);
        leases.add(host1);
        for(int j=0; j<numJobs; j++) {
            taskRequests.clear();
            taskRequests.add(TaskRequestProvider.getTaskRequest(1, memPerJob, numPortsPerJob));
            // assume ports are contiguous
            Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
            Assert.assertEquals(1, resultMap.entrySet().size());
            Assert.assertEquals(1, resultMap.values().iterator().next().getTasksAssigned().size());
            leases.clear();
            host1 = LeaseProvider.getConsumedLease(host1, 1, memPerJob, resultMap.values().iterator().next().getTasksAssigned().iterator().next().getAssignedPorts());
            leases.add(host1);
        }
    }

    @Test
    public void testMultiportJob() throws Exception {
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", 4, 4000, 1, 100);
        List<VirtualMachineLease> leases = new ArrayList<>();
        leases.add(host1);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 2, 3));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 2, 3));
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        final Set<TaskAssignmentResult> tasksAssigned = resultMap.values().iterator().next().getTasksAssigned();
        for(TaskAssignmentResult r: tasksAssigned) {
            final List<Integer> assignedPorts = r.getAssignedPorts();
            for(int p: assignedPorts)
                System.out.println(p);
        }
    }

    @Test
    public void testMultiportJob2() throws Exception {
        final VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", 4, 4000, 1, 100);
        List<VirtualMachineLease> leases = new ArrayList<>();
        leases.add(host1);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 2, 3));
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        Set<TaskAssignmentResult> tasksAssigned = resultMap.values().iterator().next().getTasksAssigned();
        for(TaskAssignmentResult r: tasksAssigned) {
            final List<Integer> assignedPorts = r.getAssignedPorts();
            for(int p: assignedPorts)
                System.out.println(p);
        }
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 2, 3));
        leases.clear();
        leases.add(LeaseProvider.getConsumedLease(resultMap.values().iterator().next()));
        resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        tasksAssigned = resultMap.values().iterator().next().getTasksAssigned();
        for(TaskAssignmentResult r: tasksAssigned) {
            final List<Integer> assignedPorts = r.getAssignedPorts();
            for(int p: assignedPorts)
                System.out.println(p);
        }
    }

    @Test
    public void testPortAllocation() throws Exception {
        VirtualMachineLease host1 = LeaseProvider.getLeaseOffer("host1", 4, 10, 1, 5);
        List<VirtualMachineLease> leases = new ArrayList<>();
        leases.add(host1);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 2, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        VMAssignmentResult result = resultMap.values().iterator().next();
        int port = result.getTasksAssigned().iterator().next().getAssignedPorts().iterator().next();
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 2, 1));
        VirtualMachineLease host11 = LeaseProvider.getLeaseOffer("host1", 3, 8, port + 1, 5);
        leases.clear();
        leases.add(host11);
        resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        result = resultMap.values().iterator().next();
        int port2 = result.getTasksAssigned().iterator().next().getAssignedPorts().iterator().next();
        Assert.assertTrue(port != port2);
    }

    @Test
    public void testMultipleOffersOnOneHost() throws Exception {
        List<VirtualMachineLease> leases = new ArrayList<>();
        String host1 = "host1";
        int cores1=2;
        int cores2=2;
        VirtualMachineLease lease1 = LeaseProvider.getLeaseOffer(host1, cores1, 20, 1, 5);
        VirtualMachineLease lease2 = LeaseProvider.getLeaseOffer(host1, cores2, 60, 6, 10);
        leases.add(lease1);
        leases.add(lease2);
        List<TaskRequest> taskRequests = new ArrayList<>();
        for(int t=0; t<cores1+cores2; t++) {
            taskRequests.add(TaskRequestProvider.getTaskRequest(1, 40, 1));
        }
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
        Assert.assertEquals(2, resultMap.values().iterator().next().getTasksAssigned().size());
    }

    @Test
    public void testMultipleHostsAndTasks() throws Exception {
        int numHosts=2;
        int numCoresPerHost=4;
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(numHosts, numCoresPerHost, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        for(int t=0; t<numCoresPerHost*numHosts; t++) {
            taskRequests.add(TaskRequestProvider.getTaskRequest(1, 1, 1));
        }
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(numHosts, resultMap.entrySet().size());
        Iterator<VMAssignmentResult> iterator = resultMap.values().iterator();
        while (iterator.hasNext()) {
            Assert.assertEquals(numCoresPerHost, iterator.next().getTasksAssigned().size());
        }
    }

    @Test
    public void testOfferReuse() throws Exception {
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(5, 10, 1));
        Map<String,VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.entrySet().size());
        leases.clear(); // don't pass the same lease again
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(4, 10, 1));
        resultMap = taskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.entrySet().size());
    }

    @Test
    public void testOfferExpiry() throws Exception {
        final AtomicBoolean leaseRejected = new AtomicBoolean(false);
        final long leaseExpirySecs=2;
        TaskScheduler myTaskScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(leaseExpirySecs)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        leaseRejected.set(true);
                    }
                })
                .build();
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(5, 10, 1));
        Map<String,VMAssignmentResult> resultMap = myTaskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.entrySet().size());
        leases.clear(); // don't pass in the same lease again.
        // wait for lease to expire
        try{Thread.sleep(leaseExpirySecs*1000+200);}catch (InterruptedException ie){}
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(4, 10, 1)); // make sure task doesn't get assigned
        resultMap = myTaskScheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(true, leaseRejected.get());
        Assert.assertEquals(0, resultMap.entrySet().size());
    }

    @Test
    public void testOfferExpiryOnSeveralSlaves() throws Exception {
        final AtomicInteger rejectCount = new AtomicInteger();
        final long leaseExpirySecs=2;
        TaskScheduler scheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(leaseExpirySecs)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        rejectCount.incrementAndGet();
                    }
                })
                .build();
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(10, 4, 4000, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(5, 10, 1));
        Map<String,VMAssignmentResult> resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.entrySet().size());
        leases.clear(); // don't pass in the same lease again.
        // wait for lease to expire
        try{Thread.sleep(leaseExpirySecs*1000+1000);}catch (InterruptedException ie){}
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(5, 10, 1)); // make sure task doesn't get assigned
        resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(0, resultMap.size());
        Assert.assertEquals(2, rejectCount.get()); // NOTE: we know only 2 leases are rejected at a time, so will have to hard code that until
        // scheduler provides a configurable value to set on it
    }

    /**
     * Test that the TaskTrackerState object passed into fitness calculator has the right jobs in it.
     * @throws Exception
     */
    @Test
    public void testTaskTrackerState1() throws Exception {
        final AtomicReference<Set<String>> runningTasks = new AtomicReference<Set<String>>(new HashSet<String>());
        final AtomicReference<Set<String>> assignedTasks = new AtomicReference<Set<String>>(new HashSet<String>());
        TaskScheduler scheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(10000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease l) {
                        Assert.fail("Unexpected lease reject called on " + l.getOffer().getHostname());
                    }
                })
                .withFitnessCalculator(new VMTaskFitnessCalculator() {
                    @Override
                    public String getName() {
                        return "DummyFitnessCalculator";
                    }
                    @Override
                    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                        Assert.assertEquals(assignedTasks.get().size(), taskTrackerState.getAllCurrentlyAssignedTasks().size());
                        Assert.assertEquals(runningTasks.get().size(), taskTrackerState.getAllRunningTasks().size());
                        assignedTasks.get().add(taskRequest.getId());
                        return 1.0; // always fits
                    }
                })
                .build();
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(1, 4, 100, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        SchedulingResult schedulingResult = scheduler.scheduleOnce(taskRequests, leases);
        Assert.assertEquals(1, schedulingResult.getResultMap().size());
        Assert.assertEquals(1, assignedTasks.get().size());
        VMAssignmentResult res = schedulingResult.getResultMap().values().iterator().next();
        TaskRequest request = res.getTasksAssigned().iterator().next().getRequest();
        scheduler.getTaskAssigner().call(request, res.getHostname());
        runningTasks.get().add(request.getId());
        assignedTasks.get().remove(request.getId());
        leases.clear();
        leases.addAll(LeaseProvider.getLeases(1, 3, 90, 2, 10));
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        taskRequests.add(TaskRequestProvider.getTaskRequest(1, 10, 1));
        schedulingResult = scheduler.scheduleOnce(taskRequests, leases);
        Assert.assertTrue(schedulingResult.getResultMap()!=null);
        Assert.assertEquals(1, schedulingResult.getResultMap().size());
        Assert.assertEquals(2, assignedTasks.get().size());

    }

}
