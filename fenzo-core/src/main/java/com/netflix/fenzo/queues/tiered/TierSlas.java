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

import java.util.HashMap;
import java.util.Map;

/* package */ class TierSlas {
    private volatile Map<Integer, TierSla> resAllocsMap = new HashMap<>();

    TierSla getTierSla(int tierIndex) {
        return resAllocsMap.get(tierIndex);
    }

    double getBucketAllocation(int tier, String bucketName) {
        final TierSla tierSla = resAllocsMap.get(tier);
        return tierSla == null ? 1.0 : tierSla.evalAllocationShare(bucketName);
    }

    void setAllocations(TieredQueueSlas slas) {
        resAllocsMap = slas.getSlas();
    }
}
