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

import java.util.function.Function;

public class AutoScaleRuleProvider {

    static AutoScaleRule createRule(final String name, final int min, final int max, final long coolDownSecs,
                                    final double cpuTooSmall, final double memoryTooSmall,
                                    Function<Integer, Integer> shortfallAdjustingFunc) {
        return new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return name;
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return min;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return max;
            }

            @Override
            public long getCoolDownSecs() {
                return coolDownSecs;
            }

            @Override
            public int getShortfallAdjustedAgents(int numberOfAgents) {
                return shortfallAdjustingFunc.apply(numberOfAgents);
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return (lease.cpuCores() < cpuTooSmall || lease.memoryMB() < memoryTooSmall);
            }
        };
    }

    static AutoScaleRule createRule(final String name, final int min, final int max, final long coolDownSecs,
                                    final double cpuTooSmall, final double memoryTooSmall) {
        return createRule(name, min, max, coolDownSecs, cpuTooSmall, memoryTooSmall, Function.identity());
    }

    static AutoScaleRule createWithMinSize(final String name, final int min, final int max, final long coolDownSecs,
                                           final double cpuTooSmall, final double memoryTooSmall, int minSize) {
        return new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return name;
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return min;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return max;
            }

            @Override
            public long getCoolDownSecs() {
                return coolDownSecs;
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return (lease.cpuCores() < cpuTooSmall || lease.memoryMB() < memoryTooSmall);
            }

            @Override
            public int getMinSize() {
                return minSize;
            }
        };
    }

    static AutoScaleRule createWithMaxSize(final String name, final int min, final int max, final long coolDownSecs,
                                           final double cpuTooSmall, final double memoryTooSmall, int maxSize) {
        return new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return name;
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return min;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return max;
            }

            @Override
            public long getCoolDownSecs() {
                return coolDownSecs;
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return (lease.cpuCores() < cpuTooSmall || lease.memoryMB() < memoryTooSmall);
            }

            @Override
            public int getMaxSize() {
                return maxSize;
            }
        };
    }
}
