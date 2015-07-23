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
 * A scale up action used by autoscaler to trigger cluster scale up for a given autoscale rule.
 */
public class ScaleUpAction implements AutoScaleAction {
    private final String ruleName;
    private final int scaleUpCount;

    ScaleUpAction(String ruleName, int scaleUpCount) {
        this.ruleName = ruleName;
        this.scaleUpCount = scaleUpCount;
    }

    /**
     * Get the type of the autoscale action, {@code Up} in this case.
     *
     * @return {@code Up}
     */
    @Override
    public Type getType() {
        return Type.Up;
    }

    /**
     * Get the name of the autoscale rule for which scale up is triggered.
     *
     * @return Name of the autoscale rule.
     */
    @Override
    public String getRuleName() {
        return ruleName;
    }

    /**
     * Get the number of hosts to add to the cluster for this autoscale rule.
     *
     * @return Number of hosts to add.
     */
    public int getScaleUpCount() {
        return scaleUpCount;
    }
}
