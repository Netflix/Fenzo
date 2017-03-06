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

import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.fenzo.functions.Action1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BinPackingSchedulerTests {
    private static final Logger logger = LoggerFactory.getLogger(BinPackingSchedulerTests.class);

    @Before
    public void setUp() throws Exception {

    }
    @After
    public void tearDown() throws Exception {

    }

    private TaskScheduler getScheduler(VMTaskFitnessCalculator fitnessCalculator) {
        return new TaskScheduler.Builder()
                .withFitnessCalculator(fitnessCalculator)
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        logger.info("Rejecting lease on " + virtualMachineLease.hostname());
                    }
                })
                .build();
    }

    @Test
    public void testCPUBinPacking1() {
        double totalCores=4;
        double usedCores=1;
        double totalMemory=100;
        double usedMemory=10;
        TaskScheduler scheduler = getScheduler(BinPackingFitnessCalculators.cpuBinPacker);
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, totalCores, totalMemory, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(TaskRequestProvider.getTaskRequest(usedCores, usedMemory, 1));
        Map<String,VMAssignmentResult> resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        String usedHostname = resultMap.keySet().iterator().next();
        leases.clear();
        leases.add(LeaseProvider.getLeaseOffer(usedHostname, totalCores-usedCores, totalMemory-usedMemory, 1, 10));
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(usedCores, usedMemory, 1));
        resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        Assert.assertEquals(usedHostname, resultMap.keySet().iterator().next());
    }

    @Test
    public void testCPUBinPacking2() {
        double totalCores=4;
        double usedCores=1;
        double totalMemory=100;
        double usedMemory=10;
        TaskRequest task1 = TaskRequestProvider.getTaskRequest(usedCores, usedMemory, 1);
        TaskRequest task2 = TaskRequestProvider.getTaskRequest(usedCores*2, usedMemory, 1);
        TaskScheduler scheduler = getScheduler(BinPackingFitnessCalculators.cpuBinPacker);
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(2, totalCores, totalMemory, 1, 10);
        List<TaskRequest> taskRequests = new ArrayList<>();
        taskRequests.add(task1);
        // First schedule just task1 with both leases, one of the leases should get used
        Map<String,VMAssignmentResult> resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        String usedHostname1 = resultMap.keySet().iterator().next();
        scheduler.getTaskAssigner().call(task1, usedHostname1);
        taskRequests.clear();
        taskRequests.add(task2);
        leases.clear();
        // Now submit task2 without any new leases. The other lease should get used
        resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        String usedHostname2 = resultMap.keySet().iterator().next();
        Assert.assertTrue(!usedHostname1.equals(usedHostname2));
        scheduler.getTaskAssigner().call(task2, usedHostname2);
        // Now add back both leases with remaining resources and submit task 3. Should go to the 2nd host's lease since
        // more of its CPUs are already in use
        leases.add(LeaseProvider.getLeaseOffer(usedHostname2, totalCores-task2.getCPUs(), totalMemory-task2.getMemory(), 2, 10));
        leases.add(LeaseProvider.getLeaseOffer(usedHostname1, totalCores-task1.getCPUs(), totalMemory-task1.getMemory(), 2, 10));
        taskRequests.clear();
        taskRequests.add(TaskRequestProvider.getTaskRequest(usedCores, usedMemory, 1));
        resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        Assert.assertTrue("CPU Bin packing failed", usedHostname2.equals(resultMap.keySet().iterator().next()));
    }

    @Test
    public void testCPUBinPackingWithSeveralHosts() {
        testBinPackingWithSeveralHosts("CPU");
    }

    @Test
    public void testMemoryBinPackingWithSeveralHosts() {
        testBinPackingWithSeveralHosts("Memory");
    }

    @Test
    public void testNetworkBinPackingWithSeveralHosts() {
        testBinPackingWithSeveralHosts("Network");
    }

    private void testBinPackingWithSeveralHosts(String resource) {
        TaskScheduler scheduler=null;
        switch (resource) {
            case "CPU":
                scheduler = getScheduler(BinPackingFitnessCalculators.cpuBinPacker);
                break;
            case "Memory":
                scheduler = getScheduler(BinPackingFitnessCalculators.memoryBinPacker);
                break;
            case "Network":
                scheduler = getScheduler(BinPackingFitnessCalculators.networkBinPacker);
                break;
            default:
                Assert.fail("Unknown resource type " + resource);
        }
        double cpuCores1=4;
        double cpuCores2=8;
        double memory1=400;
        double memory2=800;
        double network1=400;
        double network2 = 800;
        int N = 10; // #instances
        // First create N 8-core machines and then N 4-core machines
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(N, cpuCores2, memory2, network2, 1, 100);
        leases.addAll(LeaseProvider.getLeases(N, N, cpuCores1, memory1, network1, 1, 100));
        // Create as many tasks as to fill all of the 4-core machines, and then one more
        List<TaskRequest> taskRequests = new ArrayList<>();
        for(int i=0; i<N*cpuCores1+1; i++)
            taskRequests.add(TaskRequestProvider.getTaskRequest(1, memory1/cpuCores1, network1/cpuCores1, 1));
        Map<String,VMAssignmentResult> resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(N+1, resultMap.size());
        int hosts1=0;
        int hosts2=0;
        for(VMAssignmentResult result: resultMap.values()) {
            List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
            Assert.assertEquals(1, leasesUsed.size());
            if(leasesUsed.get(0).cpuCores() == cpuCores1)
                hosts1++;
            else
                hosts2++;
        }
        Assert.assertEquals(N, hosts1);
        Assert.assertEquals(1, hosts2);
    }

    /**
     * Test memory bin packing when servers have memory inversely proportional to CPUs
     * @throws Exception
     */
    @Test
    public void testMemoryBinPacking2() throws Exception {
        double cpus1=4;
        double memory1=800;
        double cpus2=8;
        double memory2=400;
        int N=10;
        // First create N servers of cpus1/memory1 and then N of cpus2/memory2
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(N, cpus1, memory1, 1, 100);
        leases.addAll(LeaseProvider.getLeases(N, N, cpus2, memory2, 1, 100));
        // create as many tasks as to fill all of the memory2 hosts, and then one more
        List<TaskRequest> taskRequests = new ArrayList<>();
        double memToAsk=100;
        for(int i=0; i<(N*memory2/memToAsk)+1; i++)
            taskRequests.add(TaskRequestProvider.getTaskRequest(1, memToAsk, 1));
        TaskScheduler scheduler = getScheduler(BinPackingFitnessCalculators.memoryBinPacker);
        SchedulingResult schedulingResult = scheduler.scheduleOnce(taskRequests, leases);
        Assert.assertEquals(N+1, schedulingResult.getResultMap().size());
        int hosts1=0;
        int hosts2=0;
        for(VMAssignmentResult result: schedulingResult.getResultMap().values()) {
            List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
            Assert.assertEquals(1, leasesUsed.size());
            if(leasesUsed.get(0).cpuCores() == cpus1)
                hosts1++;
            else
                hosts2++;
        }
        Assert.assertEquals(N, hosts2);
        Assert.assertEquals(1, hosts1);
    }

    // ToDo need a test to confirm BinPackingFitnessCalculators.getCpuAndMemoryBinPacker()

}
