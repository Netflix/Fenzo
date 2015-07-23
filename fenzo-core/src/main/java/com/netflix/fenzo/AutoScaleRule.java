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
 * per unique value of host attribute that is used for matching, see
 * {@link com.netflix.fenzo.TaskScheduler.Builder#withAutoScaleByAttributeName(String)}.
 */
public interface AutoScaleRule {
    /**
     * Value of the host attribute to match to apply this rule.
     *
     * @return value of matching host attribute
     */
    public String getRuleName();

    /**
     * Get the minimum number of idle hosts to keep. The autoscaler will trigger scale up action if the idle number of
     * hosts goes below this value.
     *
     * @return Minimum idle hosts for this rule.
     */
    public int getMinIdleHostsToKeep();

    /**
     * Get the maximum number of idle hosts to keep. The autoscaler will trigger scale down action if the idle number of
     * hosts goes higher than this value.
     *
     * @return
     */
    public int getMaxIdleHostsToKeep();

    /**
     * Get the number of seconds for cool down duration. Suppress autoscale actions for this many seconds after a previous
     * autoscale action.
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
