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

import java.util.*;

/* package */ class TierSlas {
    private volatile Map<Integer, TierSla> resAllocsMap = new HashMap<>();

    double getBucketAllocation(int tier, String bucketName) {
        final TierSla tierSla = resAllocsMap.get(tier);
        return tierSla == null? 1.0 : tierSla.evalAllocationShare(bucketName);
    }

    void setAllocations(TieredQueueSlas slas) {
        final Map<Integer, Map<String, ResAllocs>> tierSlasMap = slas.getSlas() == null? Collections.emptyMap() : slas.getSlas();
        Map<Integer, TierSla> tmpResAllocsMap = new HashMap<>();
        if (!tierSlasMap.isEmpty()) {
            for (Map.Entry<Integer, Map<String, ResAllocs>> entry: tierSlasMap.entrySet()) {
                final Map<String, ResAllocs> tierAllocs = entry.getValue();
                TierSla tierSla = new TierSla();
                tmpResAllocsMap.put(entry.getKey(), tierSla);
                for (Map.Entry<String, ResAllocs> e: tierAllocs.entrySet()) {
                    tierSla.setAlloc(e.getKey(), e.getValue());
                }
            }
        }
        resAllocsMap = tmpResAllocsMap;
    }
}
