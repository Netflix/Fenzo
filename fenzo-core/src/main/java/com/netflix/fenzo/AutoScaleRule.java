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
 * A rule that defines when to scale the number of hosts of a certain type. You define one rule for each unique
 * value of the host attribute that you have designated to differentiate autoscale groups (see the
 * {@link com.netflix.fenzo.TaskScheduler.Builder#withAutoScaleByAttributeName(String) withAutoScaleByAttributeName()}
 * task scheduler builder method).
 */
public interface AutoScaleRule {
    /**
     * Returns the value, for the group of hosts that this rule applies to, of the host attribute that you have
     * designated to differentiate autoscale groups. This acts as the name of the autoscaling group.
     *
     * @return the value of the designated host attribute, which is the name of the autoscaling group this rule
     *         applies to
     */
    String getRuleName();

    /**
     * Returns the minimum number of hosts, in the autoscale group this rule applies to, that Fenzo is to keep
     * in idle readiness. Keeping idle hosts in a standby state like this allows Fenzo to rapidly launch new
     * jobs without waiting for new instances to spin up.
     *
     * @return the minimum number of idle hosts to maintain in this autoscale group
     */
    int getMinIdleHostsToKeep();

    /**
     * Returns the minimum number of hosts to expect in the autoscale group for this rule. Fenzo will not invoke
     * scale down actions that will make the group size to go below this minimum size. A value of {@code 0} effectively
     * disables this function.
     * @return The minimum number of hosts to expect in the group, even if idle.
     */
    default int getMinSize() {
        return 0;
    }

    /**
     * Returns the maximum number of hosts, in the autoscale group this rule applies to, that Fenzo is to keep
     * in idle readiness. Keeping idle hosts in a standby state like this allows Fenzo to rapidly launch new
     * jobs without waiting for new instances to spin up.
     *
     * @return the maximum number of idle hosts to maintain in this autoscale group
     */
    int getMaxIdleHostsToKeep();

    /**
     * Returns the maximum number of hosts to expect in the autoscale group for this rule. Fenzo will not invoke
     * scale up actions that could make the group size higher than this value. A value of {@link Integer#MAX_VALUE}
     * effectively disables this function.
     * @return The maximum number of hosts to expect in this group, even if no idle hosts remain.
     */
    default int getMaxSize() {
        return Integer.MAX_VALUE;
    }

    /**
     * Returns adjusted number of agents. By default the same amount as passed in the constructor is returned.
     * During shortfall analysis, it is assumed that one tasks fits into one agent. This may result in too
     * excessive scale-up, especially if the upper bound on the task size is known in advance, and it requires
     * far less resources than a whole server.
     */
    default int getShortfallAdjustedAgents(int numberOfAgents) {
        return numberOfAgents;
    }

    /**
     * Returns the amount of time to wait from the beginning of a scale up or scale down operation before
     * initiating another autoscale operation (a.k.a the "cool down" time). Suppress autoscale actions for this
     * many seconds after a previous autoscale action.
     *
     * @return the cool down time, in seconds
     */
    long getCoolDownSecs();

    /**
     * Determines whether a host has too few resources to be considered an idle but potentially useful host.
     * This is used to filter out hosts with too few resources before considering them to be excess resources.
     * If they are not filtered out, they could prevent a much-needed scale up action.
     *
     * @param lease the lease object that representes the host
     * @return {@code true} if the idle machine has too few resources to count as idle, {@code false} otherwise
     */
    boolean idleMachineTooSmall(VirtualMachineLease lease);
}
