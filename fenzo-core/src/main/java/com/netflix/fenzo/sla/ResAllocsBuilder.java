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

package com.netflix.fenzo.sla;

/**
 * @warn class description missing
 */
public class ResAllocsBuilder {
    private double cores=Double.MAX_VALUE;
    private double memory=Double.MAX_VALUE;
    private double networkMbps=Double.MAX_VALUE;
    private double disk=Double.MAX_VALUE;
    private final String taskGroupName;

    public ResAllocsBuilder(String taskGroupName) {
        this.taskGroupName = taskGroupName;
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param cores
     * @return
     */
    public ResAllocsBuilder withCores(double cores) {
        this.cores = cores;
        return this;
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param memory
     * @return
     */
    public ResAllocsBuilder withMemory(double memory) {
        this.memory = memory;
        return this;
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param networkMbps
     * @return
     */
    public ResAllocsBuilder withNetworkMbps(double networkMbps) {
        this.networkMbps = networkMbps;
        return this;
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param disk
     * @return
     */
    public ResAllocsBuilder withDisk(double disk) {
        this.disk = disk;
        return this;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public ResAllocs build() {
        final double c = cores;
        final double m = memory;
        final double n = networkMbps;
        final double d = disk;
        return new ResAllocs() {
            @Override
            public String getTaskGroupName() {
                return taskGroupName;
            }

            @Override
            public double getCores() {
                return c;
            }

            @Override
            public double getMemory() {
                return m;
            }

            @Override
            public double getNetworkMbps() {
                return n;
            }

            @Override
            public double getDisk() {
                return d;
            }
        };
    }
}
