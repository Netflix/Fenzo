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
 * An autoscale action indicating that the autoscale group is to be scaled up.
 */
public class ScaleUpAction implements AutoScaleAction {
    private final String ruleName;
    private final int scaleUpCount;

    ScaleUpAction(String ruleName, int scaleUpCount) {
        this.ruleName = ruleName;
        this.scaleUpCount = scaleUpCount;
    }

    /**
     * Returns an indication of whether the autoscale action is to scale up or to scale down - in this case, up.
     * 
     * @return {@link AutoScaleAction.Type#Up Up}
     */
    @Override
    public Type getType() {
        return Type.Up;
    }

    /**
     * Returns the name of the autoscale rule that triggered the scale up action.
     *
     * @return the name of the autoscale rule
     */
    @Override
    public String getRuleName() {
        return ruleName;
    }

    /**
     * Returns the number of hosts to add to the cluster during this scale up action.
     *
     * @return the number of hosts to add
     */
    public int getScaleUpCount() {
        return scaleUpCount;
    }
}
