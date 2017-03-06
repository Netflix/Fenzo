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

import com.netflix.fenzo.queues.TaskQueueSla;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsUtil;

import java.util.HashMap;
import java.util.Map;

public class TieredQueueSlas implements TaskQueueSla {
    private final Map<Integer, TierSla> slas;

    public TieredQueueSlas(Map<Integer, ResAllocs> tierCapacities, Map<Integer, Map<String, ResAllocs>> slas) {
        Map<Integer, TierSla> tmpResAllocsMap = new HashMap<>();

        if (!slas.isEmpty()) {
            for (Map.Entry<Integer, Map<String, ResAllocs>> entry : slas.entrySet()) {
                int tierNumber = entry.getKey();
                final Map<String, ResAllocs> tierAllocs = entry.getValue();

                TierSla tierSla = new TierSla();
                tierSla.setTierCapacity(getOrComputeTierCapacity(tierNumber, tierCapacities.get(tierNumber), tierAllocs));

                for (Map.Entry<String, ResAllocs> e : tierAllocs.entrySet()) {
                    tierSla.setAlloc(e.getKey(), e.getValue());
                }

                tmpResAllocsMap.put(tierNumber, tierSla);
            }
        }

        tierCapacities.forEach((tierIndex, capacity) -> {
            if (!tmpResAllocsMap.containsKey(tierIndex)) {
                TierSla tierSla = new TierSla();
                tierSla.setTierCapacity(tierCapacities.get(tierIndex));
                tmpResAllocsMap.put(tierIndex, tierSla);
            }
        });

        this.slas = tmpResAllocsMap;
    }

    private ResAllocs getOrComputeTierCapacity(int tierNumber, ResAllocs tierCapacity, Map<String, ResAllocs> queueAllocs) {
        if (tierCapacity != null) {
            return tierCapacity;
        }
        String tierName = "tier#" + tierNumber;
        if (queueAllocs.isEmpty()) {
            return ResAllocsUtil.emptyOf(tierName);
        }
        return queueAllocs.values().stream().reduce(ResAllocsUtil.emptyOf(tierName), ResAllocsUtil::add);
    }

    /* package */ Map<Integer, TierSla> getSlas() {
        return slas;
    }
}
