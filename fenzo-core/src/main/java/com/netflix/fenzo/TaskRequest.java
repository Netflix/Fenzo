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

import java.util.List;

/**
 * @warn interface description missing
 */
public interface TaskRequest {
    /**
     * @warn method description missing
     *
     * @return
     */
    public String getId();

    /**
     * @warn method description missing
     *
     * @return
     */
    public String taskGroupName();

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getCPUs();

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getMemory();

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getNetworkMbps();

    /**
     * @warn method description missing
     *
     * @return
     */
    public double getDisk();

    /**
     * @warn method description missing
     *
     * @return
     */
    public int getPorts();

    /**
     * @warn method description missing
     *
     * @return
     */
    public List<? extends ConstraintEvaluator> getHardConstraints();

    /**
     * @warn method description missing
     *
     * @return
     */
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints();
}
