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
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.fenzo.plugins.SpreadingFitnessCalculators;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpreadingSchedulerTests {
    private static final Logger logger = LoggerFactory.getLogger(SpreadingSchedulerTests.class);

    private TaskScheduler getScheduler(VMTaskFitnessCalculator fitnessCalculator) {
        return new TaskScheduler.Builder()
                .withFitnessCalculator(fitnessCalculator)
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(virtualMachineLease -> logger.info("Rejecting lease on " + virtualMachineLease.hostname()))
                .build();
    }

    /**
     * Tests whether or not the tasks will be spread out across the hosts based on cpu such that each host has a single task.
     */
    @Test
    public void testCPUBinPackingWithSeveralHosts() {
        testBinPackingWithSeveralHosts("CPU");
    }

    /**
     * Tests whether or not the tasks will be spread out across the hosts based on memory such that each host has a single task.
     */
    @Test
    public void testMemoryBinPackingWithSeveralHosts() {
        testBinPackingWithSeveralHosts("Memory");
    }

    /**
     * Tests whether or not the tasks will be spread out across the hosts based on network such that each host has a single task.
     */
    @Test
    public void testNetworkBinPackingWithSeveralHosts() {
        testBinPackingWithSeveralHosts("Network");
    }

    private void testBinPackingWithSeveralHosts(String resource) {
        TaskScheduler scheduler = null;
        switch (resource) {
            case "CPU":
                scheduler = getScheduler(SpreadingFitnessCalculators.cpuSpreader);
                break;
            case "Memory":
                scheduler = getScheduler(SpreadingFitnessCalculators.memorySpreader);
                break;
            case "Network":
                scheduler = getScheduler(SpreadingFitnessCalculators.networkSpreader);
                break;
            default:
                Assert.fail("Unknown resource type " + resource);
        }
        double cpuCores = 4;
        double memory = 1024;
        double network = 1024;
        int numberOfInstances = 10;

        List<VirtualMachineLease> leases = LeaseProvider.getLeases(numberOfInstances, cpuCores, memory, network, 1, 100);
        List<TaskRequest> taskRequests = new ArrayList<>();
        for (int i = 0; i < numberOfInstances; i++) {
            taskRequests.add(TaskRequestProvider.getTaskRequest(1, 256, 256, 0));
        }
        Map<String, VMAssignmentResult> resultMap = scheduler.scheduleOnce(taskRequests, leases).getResultMap();
        Assert.assertEquals(resultMap.size(), numberOfInstances);

        Map<String, Long> assignmentCountPerHost = resultMap.values().stream().collect(Collectors.groupingBy(
                VMAssignmentResult::getHostname, Collectors.counting()));

        boolean duplicates = assignmentCountPerHost.entrySet().stream().anyMatch(e -> e.getValue() > 1);
        Assert.assertEquals(duplicates, false);
    }
}
