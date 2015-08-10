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
 * Describes an autoscaling action, either the scaling up or down of an autoscaling group.
 */
public interface AutoScaleAction {
    /**
     * Indicates whether the autoscale action is to scale up or to scale down.
     */
    public enum Type {Up, Down};

    /**
     * Get the name of the auto scale rule that is triggering the autoscale action.
     *
     * @return name of the autoscale rule
     */
    public String getRuleName();

    /**
     * Returns an indication of whether the autoscale action is to scale up or to scale down.
     *
     * @return an enum indicating whether the autoscale action is {@link Type#Up} or {@link Type#Down}
     */
    public Type getType();
}
