/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsUtil;

import java.util.HashMap;
import java.util.Map;

/* package */ class TierSla {

    // small allocation for a bucket with no defiend allocation
    static final double eps = 0.001;

    private ResAllocs tierCapacity;

    private final Map<String, ResAllocs> allocsMap = new HashMap<>();
    private double totalCpu = 0.0;
    private double totalMem = 0.0;
    private double totalNetwork = 0.0;
    private double totalDisk = 0.0;

    void setTierCapacity(ResAllocs tierCapacity) {
        this.tierCapacity  = tierCapacity;
    }

    void setAlloc(String bucket, ResAllocs value) {
        final ResAllocs prev = allocsMap.put(bucket, value);
        if (prev != null)
            subtract(prev);
        add(value);
    }

    private void add(ResAllocs value) {
        totalCpu += value.getCores();
        totalMem += value.getMemory();
        totalNetwork += value.getNetworkMbps();
        totalDisk += value.getDisk();
    }

    private void subtract(ResAllocs value) {
        totalCpu -= value.getCores();
        totalMem -= value.getMemory();
        totalNetwork -= value.getNetworkMbps();
        totalDisk -= value.getDisk();
    }

    public ResAllocs getTierCapacity() {
        return tierCapacity;
    }

    ResAllocs getBucketAllocs(String bucketName) {
        return allocsMap.computeIfAbsent(bucketName, name -> ResAllocsUtil.emptyOf(bucketName));
    }

    Map<String, ResAllocs> getAllocsMap() {
        return allocsMap;
    }

    /**
     * Evaluate the allocation share of a bucket among all the buckets for which allocations are defined. If there are
     * no allocations setup, return 1.0, implying 100%. If no allocation is setup for the given <code>bucket</code>,
     * return a small value. Otherwise, calculate the share percentage of each resource cpu, memory, network, and disk
     * from the total and return the maximum of these shares.
     *
     * @param bucket Name of the bucket.
     * @return Allocation share for the bucket.
     */
    double evalAllocationShare(String bucket) {
        if (allocsMap.isEmpty()) {
            return 1.0; // special case if there are no allocations setup
        }
        final ResAllocs resAllocs = allocsMap.get(bucket);
        if (resAllocs == null)
            return totalCpu < (1.0 / eps) ? eps : 1.0 / totalCpu; // arbitrarily base it on cpus
        double val = totalCpu < 1.0 ? eps : resAllocs.getCores() / totalCpu;
        val = Math.max(val, totalMem < 1.0 ? eps : resAllocs.getMemory() / totalMem);
        val = Math.max(val, totalNetwork < 1.0 ? eps : resAllocs.getNetworkMbps() / totalNetwork);
        return Math.max(val, totalDisk < 1.0 ? eps : resAllocs.getDisk() / totalDisk);
    }
}
