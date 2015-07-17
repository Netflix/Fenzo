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
 * A rule that defines the behavior for autoscaling the number of hosts of a certain type. You define one rule
 * for each value of that host attribute that you use to identify the type of the host.
 */
public interface AutoScaleRule {
    /**
     * Returns the value of the host attribute for those hosts that this rule applies to (for example, the name
     * of the autoscaling group).
     *
     * @return value of matching host attribute
     */
    public String getRuleName();

    /**
     * Returns the minimum number of hosts of the type this rule applies to that Fenzo is to aim to keep in idle
     * readiness. Keeping idle hosts in a standby state like this allows Fenzo to rapidly launch new jobs without
     * waiting for new instances to spin up.
     *
     * @return the minimum number of idle hosts of this type
     */
    public int getMinIdleHostsToKeep();

    /**
     * Returns the maximum number of hosts of the type this rule applies to that Fenzo is to aim to keep in idle
     * readiness. Keeping idle hosts in a standby state like this allows Fenzo to rapidly launch new jobs without
     * waiting for new instances to spin up.
     *
     * @return the maximum number of idle hosts of this type
     */
    public int getMaxIdleHostsToKeep();

    /**
     * Returns the amount of time to wait from the beginning of a scale up or scale down operation before
     * initiating another autoscale operation.
     *
     * @return the cool down time, in seconds
     */
    public long getCoolDownSecs();

    /**
     * A predicate with which one can check to see if a technically-idle host has too few resources to be
     * considered effectively idle. This is used to filter out hosts with too few resources before considering
     * them to be excess resources. If they are not filtered out, they could prevent a much-needed scale up
     * action.
     *
     * @param lease the lease object of the VM
     * @return {@code true} if the idle machine has too few resources to count as idle, {@code false} otherwise
     */
    public boolean idleMachineTooSmall(VirtualMachineLease lease);
}
