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

import java.util.Collection;

/**
 * An autoscale action that indicates an autoscale group is to be scaled down.
 */
public class ScaleDownAction implements AutoScaleAction {
    private final String ruleName;
    private final Collection<String> hosts;

    ScaleDownAction(String ruleName, Collection<String> hosts) {
        this.ruleName = ruleName;
        this.hosts = hosts;
        // ToDo need to ensure those hosts' offers don't get used
    }

    /**
     * Get the name of the autoscale rule that triggered the scale down action.
     *
     * @return the name of the autoscale rule.
     */
    @Override
    public String getRuleName() {
        return ruleName;
    }

    /**
     * Returns an indication of whether the autoscale action is to scale up or to scale down - in this case,
     * down.
     *
     * @return {@link AutoScaleAction.Type#Down Down}
     */
    @Override
    public Type getType() {
        return Type.Down;
    }

    /**
     * Get the hostnames to unqueueTask from the cluster during the scale down action.
     *
     * @return a Collection of host names.
     */
    public Collection<String> getHosts() {
        return hosts;
    }
}
