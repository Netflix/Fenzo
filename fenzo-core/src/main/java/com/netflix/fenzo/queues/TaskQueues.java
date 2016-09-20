/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.fenzo.queues;

import com.netflix.fenzo.queues.tiered.TieredQueue;

/**
 * A class to create instances of various {@link TaskQueue} implementations.
 */
public class TaskQueues {

    /**
     * Create a tiered {@link TaskQueue} with the given number of tiers.
     * @param numTiers The number of tiers.
     * @return A {@link TaskQueue} implementation that supports tiered queues.
     */
    public static TaskQueue createTieredQueue(int numTiers) {
        return new TieredQueue(numTiers);
    }
}
