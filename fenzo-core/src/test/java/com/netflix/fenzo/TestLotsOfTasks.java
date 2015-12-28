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
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A crude sample that tests Fenzo assignment speed with lots of mocked hosts and lots of tasks that are iteratively
 * assigned in batches, like how a real world system may get tasks submitted routinely. Quantities that can be varied
 * in the code include number of hosts, number of CPUs per host, number of tasks to assign per iteration, and Fenzo's
 * "fitness good enough" value. The main() method creates the hosts' offers (mocked) and enough tasks to fill all of
 * the hosts. It creates tasks of three different kinds - 1, single cpu task, tasks asking for half the number of total
 * CPUs per host, and tasks asking for 0.75 times the number of CPUs per host. Then, it iteratively assigns resources
 * to "batch" number of tasks until all tasks have been tried for assignment. At the end, it prints the average time
 * taken for each batch's assignment in Fenzo. It "primes" the assignment routine by not including the time taken for
 * the first few iterations. Also, it prints how many tasks were not assigned any resources and the
 * total fill/utilization of the resources. Because of the way tasks may get assigned resources and the fitness strategy
 * used, the utilization may not be 100%.
 */
public class TestLotsOfTasks {
    private int numHosts;
    private int numCores;
    private double memory;

    private List<TaskRequest> getTasks() {
        // Add some single-core, some half machine sized, and some three-quarters machine sized tasks
        List<TaskRequest> requests = new ArrayList<>();
        double fractionSingleCore=0.2;
        double fractionHalfSized=0.4;
        double fractionThreeQuarterSized = 1.0 - fractionHalfSized - fractionSingleCore;
        int numCoresUsed=0;
        for(int t=0; t<numHosts*numCores*fractionSingleCore; t++, numCoresUsed++)
            requests.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        System.out.println("numCoresRequested=" + numCoresUsed);
        for(int t=0; t<(numCores*numHosts*fractionHalfSized/(numCores/2)); t++) {
            requests.add(TaskRequestProvider.getTaskRequest(numCores/2, numCores*1000/2, 1));
            numCoresUsed += numCores/2;
        }
        System.out.println("numCoresRequested=" + numCoresUsed);
        for(int t=0; t<(numCores*numHosts*fractionThreeQuarterSized/(numCores*0.75)); t++) {
            requests.add(TaskRequestProvider.getTaskRequest(numCores*0.75, numCores*1000*0.75, 1));
            numCoresUsed += numCores*0.75;
        }
        // fill remaining cores with single-core tasks to get 100% potential utilization
        System.out.println("#Tasks=" + requests.size() + ", numCoresRequested=" + numCoresUsed + " of possible " + (numCores*numHosts));
        for(int t=0; t<(numCores*numHosts-numCoresUsed); t++)
            requests.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        List<TaskRequest> result = new ArrayList<>();
        // randomly add into result from requests
        for(int i=0; i<20; i++) {
            Iterator<TaskRequest> iterator = requests.iterator();
            while(iterator.hasNext()) {
                if((int)(Math.random()*10) % 2 == 0) {
                    result.add(iterator.next());
                    iterator.remove();
                }
                else
                    iterator.next();
            }
        }
        // add all remaining tasks
        result.addAll(requests);
        return result;
    }

    private List<VirtualMachineLease> getLeases() {
        return LeaseProvider.getLeases(numHosts, numCores, memory, 1, 10);
    }

    private static final double GOOD_ENOUGH_FITNESS=0.5;

    // Results looks like this;
    //
    // ------------------------------------------------------------------------------------
    // | Fitness     | #Hosts | #CPUs    | #tasks to assign   | Avg mSecs | Utilization % |
    // | Good Enough |        | per host | per scheduling run | per run   |               |
    // ------------------------------------------------------------------------------------
    // |  0.01       | 10,000 |   8      |  200               |      58   |    97.14      |
    // |  0.1        | 10,000 |   8      |  200               |      56   |    97.19      |
    // |  0.5        | 10,000 |   8      |  200               |     145   |    97.24      |
    // |  1.0        | 10,000 |   8      |  200               |     246   |    97.11      |
    // ------------------------------------------------------------------------------------
    // |  0.01       |  2,000 |   8      |  200               |      31   |    97.09      |
    // |  0.1        |  2,000 |   8      |  200               |      30   |    97.45      |
    // |  0.5        |  2,000 |   8      |  200               |      35   |    97.00      |
    // |  1.0        |  2,000 |   8      |  200               |      54   |    97.23      |
    // ------------------------------------------------------------------------------------
    // |  0.01       |    200 |   8      |  200               |      59   |    97.13      |
    // |  0.1        |    200 |   8      |  200               |      57   |    97.00      |
    // |  0.5        |    200 |   8      |  200               |      45   |    97.00      |
    // |  1.0        |    200 |   8      |  200               |      58   |    96.50      |
    // ------------------------------------------------------------------------------------
    //


    public static void main(String[] args) {
        TaskScheduler scheduler = getTaskScheduler();
        TestLotsOfTasks tester = new TestLotsOfTasks();
        tester.numHosts=2000;
        tester.numCores=16;
        tester.memory=1000*tester.numCores;
        List<TaskRequest> tasks = tester.getTasks();
        List<VirtualMachineLease> leases = tester.getLeases();
        test2(tester, scheduler, tasks, leases);
        scheduler.shutdown();
        System.out.println("ALL DONE");
    }

    private static void addToAsgmtMap(Map<String, List<TaskRequest>> theMap, String hostname, TaskRequest request) {
        if(theMap.get(hostname)==null)
            theMap.put(hostname, new ArrayList<TaskRequest>());
        theMap.get(hostname).add(request);
    }

    private static void test2(TestLotsOfTasks tester, TaskScheduler scheduler, List<TaskRequest> tasks,
                              List<VirtualMachineLease> leases) {
        // schedule 1 task first
        int n=0;
        double totalAssignedCpus=0.0;
        Map<String, TaskRequest> jobIds = new HashMap<>();
        Map<String, List<TaskRequest>> assignmentsMap = new HashMap<>();
        for(TaskRequest r: tasks)
            jobIds.put(r.getId(), r);
        int totalNumAllocations=0;
        SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks.subList(n++, n), leases);
        totalNumAllocations += schedulingResult.getNumAllocations();
        VMAssignmentResult assignmentResult = schedulingResult.getResultMap().values().iterator().next();
        TaskAssignmentResult taskAssignmentResult = assignmentResult.getTasksAssigned().iterator().next();
        TaskRequest request = taskAssignmentResult.getRequest();
        if(jobIds.remove(request.getId()) == null)
            System.err.println("    Removed " + request.getId() + " already!");
        else
            totalAssignedCpus += request.getCPUs();
        addToAsgmtMap(assignmentsMap, assignmentResult.getHostname(), request);
        //System.out.println(assignmentResult.getHostname() + " : " + request.getId());
        scheduler.getTaskAssigner().call(request, assignmentResult.getHostname());
        VirtualMachineLease consumedLease = LeaseProvider.getConsumedLease(assignmentResult.getLeasesUsed().iterator().next(), request.getCPUs(),
                request.getMemory(), taskAssignmentResult.getAssignedPorts());
        for(int i=0; i<4; i++) {
            leases.clear();
            if(consumedLease.cpuCores()>0.0 && consumedLease.memoryMB()>0.0)
                leases.add(consumedLease);
            schedulingResult = scheduler.scheduleOnce(tasks.subList(n++, n), leases);
            totalNumAllocations += schedulingResult.getNumAllocations();
            assignmentResult = schedulingResult.getResultMap().values().iterator().next();
            taskAssignmentResult = assignmentResult.getTasksAssigned().iterator().next();
            request = taskAssignmentResult.getRequest();
            if(jobIds.remove(request.getId()) == null)
                System.err.println("    Removed " + request.getId() + " already!");
            else
                totalAssignedCpus += request.getCPUs();
            addToAsgmtMap(assignmentsMap, assignmentResult.getHostname(), request);
            //System.out.println(assignmentResult.getHostname() + " : " + request.getId());
            scheduler.getTaskAssigner().call(request, assignmentResult.getHostname());
            consumedLease = LeaseProvider.getConsumedLease(assignmentResult.getLeasesUsed().iterator().next(),
                    request.getCPUs(), request.getMemory(), taskAssignmentResult.getAssignedPorts());
        }
        int tasksLeft = tasks.size() - n;
        //System.out.println("#Tasks left = " + tasksLeft);
        long max=0;
        long min=0;
        double sum=0.0;
        int numIters=0;
        int totalTasksAssigned=n;
        leases.clear();
        if(consumedLease.cpuCores()>0.0 && consumedLease.memoryMB()>0.0)
            leases.add(consumedLease);
        long st = 0;
        long totalTime=0;
        int batchSize=200;
        boolean first=true;
        String lastJobId="-1";
        while(n < tasks.size()) {
            numIters++;
            int until = Math.min(n + batchSize, tasks.size());
            st = System.currentTimeMillis();
            //System.out.println(n + " -> " + until);
            schedulingResult = scheduler.scheduleOnce(tasks.subList(n, until), leases);
            totalNumAllocations += schedulingResult.getNumAllocations();
            leases.clear();
            int assigned=0;
            for(VMAssignmentResult result: schedulingResult.getResultMap().values()) {
                double usedCpus=0.0;
                double usedMem=0.0;
                List<Integer> portsUsed = new ArrayList<>();
                for(TaskAssignmentResult t: result.getTasksAssigned()) {
                    if(jobIds.remove(t.getRequest().getId()) == null)
                        System.err.println("    Removed " + t.getRequest().getId() + " already!");
                    lastJobId = t.getRequest().getId();
                    addToAsgmtMap(assignmentsMap, result.getHostname(), t.getRequest());
                    assigned++;
                    scheduler.getTaskAssigner().call(t.getRequest(), result.getHostname());
                    totalAssignedCpus += t.getRequest().getCPUs();
                    usedCpus += t.getRequest().getCPUs();
                    usedMem += t.getRequest().getMemory();
                    portsUsed.addAll(t.getAssignedPorts());
//                    StringBuffer buf = new StringBuffer("    host " + result.getHostname() + " task " + t.getTaskId() + " ports: ");
//                    for(Integer p: t.getAssignedPorts())
//                        buf.append(""+p).append(", ");
//                    System.out.println(buf.toString());
                }
                consumedLease = LeaseProvider.getConsumedLease(result);
                if(consumedLease.cpuCores()>0.0 && consumedLease.memoryMB()>0.0)
                    leases.add(consumedLease);
            }
            //printResourceStatus(scheduler.getResourceStatus());
            long delta = System.currentTimeMillis()-st;
            //System.out.println("                    delta = " + delta);
            if(first)
                first=false; // skip time measurements the first time
            else {
                totalTime += delta;
                if(delta>max)
                    max=delta;
                if(min==0.0 || min>delta)
                    min = delta;
            }
            //System.out.println(assigned + " of " + (until-n) + " tasks assigned using " + allocationsCounter.get() + " allocations");
            totalTasksAssigned += assigned;
            n = until;
        }
        System.out.printf("Scheduling time total=%d, avg=%8.2f (min=%d, max=%d) from %d iterations of %d tasks each\n",
                totalTime, ((double) totalTime / Math.max(1, (numIters - 1))), min, max, numIters-1, batchSize);
        System.out.println("Total tasks assigned: " + totalTasksAssigned + " of " + tasks.size()
                + " total #allocations=" + totalNumAllocations);
        System.out.println("Total CPUs assigned = " + totalAssignedCpus);
        int numHosts=0;
        double ununsedMem=0.0;
        double unusedCpus=0.0;
        for(Map.Entry<String, Map<VMResource, Double[]>> entry: scheduler.getResourceStatus().entrySet()) {
            numHosts++;
            StringBuilder buf = new StringBuilder("    host ").append(entry.getKey()).append(": ");
            boolean hasAvailCpu=true;
            boolean hasAvailMem=true;
            for(Map.Entry<VMResource, Double[]> resourceEntry: entry.getValue().entrySet()) {
                switch (resourceEntry.getKey()) {
                    case CPU:
                        if(resourceEntry.getValue()[0]<tester.numCores) {
                            hasAvailCpu=false;
                            unusedCpus += tester.numCores-resourceEntry.getValue()[0];
                        }
                        break;
                    case Memory:
                        if(resourceEntry.getValue()[0]<tester.numCores*1000) {
                            hasAvailMem=false;
                            ununsedMem += tester.numCores*1000 - resourceEntry.getValue()[0];
                        }
                        break;
                }
                buf.append(resourceEntry.getKey()).append(": used=").append(resourceEntry.getValue()[0])
                        .append(", available=").append(resourceEntry.getValue()[1]).append(",");
            }
            if(!hasAvailCpu || !hasAvailMem) {
                //System.out.println("    " + buf);
//                for(TaskRequest r: assignmentsMap.get(entry.getKey())) {
//                    System.out.println("       task " + r.getId() + " cpu=" + r.getCPUs() + ", mem=" + r.getMemory());
//                }
            }
        }
        double util = (double)(tester.numCores*tester.numHosts-unusedCpus)*100.0/(tester.numCores*tester.numHosts);
        System.out.printf("Utilization: %5.2f%%\n", util);
        if(!jobIds.isEmpty()) {
            System.out.printf("  Unused CPUs=%d, Memory=%d\n",  (int)unusedCpus, (int)ununsedMem);
            System.out.println("  Unassigned tasks:");
            for(Map.Entry<String, TaskRequest> entry: jobIds.entrySet()) {
                System.out.println("    Task " + entry.getKey() + " cpu=" + entry.getValue().getCPUs()
                        + ", mem=" + entry.getValue().getMemory());
            }
        }
        System.out.println("Total numHosts=" + numHosts);
    }

    private static void printResourceStatus(Map<String, Map<VMResource, Double[]>> resourceStatus) {
        System.out.println("*****************");
        for(Map.Entry<String, Map<VMResource, Double[]>> hostResourceEntry: resourceStatus.entrySet()) {
            for(Map.Entry<VMResource, Double[]> resourceEntry: hostResourceEntry.getValue().entrySet()) {
                if(resourceEntry.getKey()==VMResource.CPU) {
                    System.out.printf("    %s: used %3.1f of %3.1f\n", hostResourceEntry.getKey(), resourceEntry.getValue()[0], resourceEntry.getValue()[1]);
                }
            }
        }
    }

    private static void test1(TaskScheduler scheduler, List<TaskRequest> tasks, List<VirtualMachineLease> leases,
                              AtomicLong allocationsCounter) {
        int numTasksAssigned=0;
        int numHostsUsed=0;
//        Map<String,VMAssignmentResult> resultMap = scheduler.scheduleOnce(tasks, leases).getResultMap();
//        System.out.println("Used " + resultMap.size() + " hosts of " + leases.size());
//        int n=0;
//        for(VMAssignmentResult result: resultMap.values())
//            n += result.getTasksAssigned().size();
//        System.out.println("Assigned " + n + " tasks of " + tasks.size());
        for(int i=0; i<5; i++) {
            Map<String,VMAssignmentResult> resultMap = scheduler.scheduleOnce(tasks, leases).getResultMap();
            numHostsUsed = resultMap.size();
            numTasksAssigned=0;
            for(VMAssignmentResult result: resultMap.values())
                numTasksAssigned += result.getTasksAssigned().size();
        }
        long st = System.currentTimeMillis();
        int numIters=10;
        allocationsCounter.set(0);
        for(int i=0; i<numIters; i++) {
            SchedulingResult schedulingResult = scheduler.scheduleOnce(tasks, leases);
            Map<String,VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            numHostsUsed = resultMap.size();
            numTasksAssigned=0;
            for(VMAssignmentResult result: resultMap.values())
                numTasksAssigned += result.getTasksAssigned().size();
        }
        long en = System.currentTimeMillis();
        System.out.printf("Took %8.2f mS per scheduling iteration over %d iteration\n", ((double)(en-st)/(double)numIters), numIters);
        System.out.printf("Allocation trials per iteration: %6.2f\n", (double) allocationsCounter.get() / numIters);
        System.out.println("numHosts used=" + numHostsUsed + " of " + leases.size());
        System.out.println("numTasks assigned=" + numTasksAssigned + " of " + tasks.size());
    }

    private static TaskScheduler getTaskScheduler() {
        return new TaskScheduler.Builder()
                    .withFitnessGoodEnoughFunction(new Func1<Double, Boolean>() {
                        @Override
                        public Boolean call(Double aDouble) {
                            return aDouble >= GOOD_ENOUGH_FITNESS;
                        }
                    })
                    .withFitnessCalculator(BinPackingFitnessCalculators.cpuBinPacker)
                    .withLeaseOfferExpirySecs(1000000)
                    .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                        @Override
                        public void call(VirtualMachineLease lease) {
                            System.err.println("Unexpected to reject lease on " + lease.hostname());
                        }
                    })
                    .build();
    }

    private static class BinSpreader implements VMTaskFitnessCalculator {
        private VMTaskFitnessCalculator binPacker = BinPackingFitnessCalculators.cpuMemBinPacker;
        @Override
        public String getName() {
            return "Bin Spreader";
        }
        @Override
        public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
            return 1.0 - binPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
        }
    }

}
