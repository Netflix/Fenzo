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

/**
 * A rule to define the behavior for auto scaling the number of hosts of a certain type. Rules are defined
 * per unique value of host attribute that is used for matching.
 */
public interface AutoScaleRule {
    /**
     * Value of the host attribute to match to apply this rule.
     *
     * @return value of matching host attribute
     */
    public String getRuleName();

    /**
     * @warn method description missing
     *
     * @return
     */
    public int getMinIdleHostsToKeep();

    /**
     * @warn method description missing
     *
     * @return
     */
    public int getMaxIdleHostsToKeep();

    /**
     * @warn method description missing
     *
     * @return
     */
    public long getCoolDownSecs();

    /**
     * Predicate to check if an idle host has too few resources to be considered idle. This is used to filter out
     * hosts with too few resources before considering them as excess resources. If not filtered out, they could
     * prevent a much needed scale up action.
     *
     * @param lease the lease object of the VM
     * @return {@code true} if the idle machine is too small, {@code false} otherwise
     */
    public boolean idleMachineTooSmall(VirtualMachineLease lease);
}
