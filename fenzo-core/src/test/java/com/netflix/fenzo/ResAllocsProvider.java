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

import com.netflix.fenzo.sla.ResAllocs;

public class ResAllocsProvider {

    static ResAllocs create(final String name, final double cores, final double memory, final double network, final double disk) {
        return new ResAllocs() {
            @Override
            public String getTaskGroupName() {
                return name;
            }
            @Override
            public double getCores() {
                return cores;
            }
            @Override
            public double getMemory() {
                return memory;
            }
            @Override
            public double getNetworkMbps() {
                return network;
            }
            @Override
            public double getDisk() {
                return disk;
            }
        };
    }
}
