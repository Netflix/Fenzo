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

import java.util.Map;

public interface TierSla {

    /**
     * Get the tier number that this sla is associated with.
     * @return The tier number.
     */
    int getTierNumber();

    /**
     * Get the map with keys containing the tier bucket names and values containing the corresponding resource
     * allocations that is set as the initial allocations for the tier bucket. A tier bucket not containing a non-null
     * entry in the map is said to have no resource allocations set.
     * @return Map of bucket names to initial resource allocations.
     */
    Map<String, ResAllocs> getInitialAllocsMap();

    /**
     * Get the resolved map of resource allocations for this tier. The resource allocations are resolved using the
     * initial allocations map and the total resources possibly available for this tier.
     * @return Map of bucket names to resolved resource allocations.
     */
    Map<String, ResAllocs> getResolvedAllocsMap();
}
