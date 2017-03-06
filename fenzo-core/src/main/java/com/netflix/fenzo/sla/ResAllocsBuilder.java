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
 * Builder class for {@link ResAllocs}.
 */
public class ResAllocsBuilder {
    private double cores = Double.MAX_VALUE;
    private double memory = Double.MAX_VALUE;
    private double networkMbps = Double.MAX_VALUE;
    private double disk = Double.MAX_VALUE;
    private final String taskGroupName;

    public ResAllocsBuilder(String taskGroupName) {
        this.taskGroupName = taskGroupName;
    }

    /**
     * Limits the number of cores the task group can use to the number you pass in to this method.
     *
     * @param cores the maximum number of CPUs
     * @return the same {@code ResAllocsBuilder}, modified accordingly
     */
    public ResAllocsBuilder withCores(double cores) {
        this.cores = cores;
        return this;
    }

    /**
     * Limits the amount of memory the task group can use to the number of MB you pass in to this method.
     *
     * @param memory the maximum amount of memory, in MB
     * @return the same {@code ResAllocsBuilder}, modified accordingly
     */
    public ResAllocsBuilder withMemory(double memory) {
        this.memory = memory;
        return this;
    }

    /**
     * Limits the amount of bandwidth the task group can use to the number of megabits per second you pass in to
     * this method.
     *
     * @param networkMbps the maximum about of bandwidth, in Mbps
     * @return the same {@code ResAllocsBuilder}, modified accordingly
     */
    public ResAllocsBuilder withNetworkMbps(double networkMbps) {
        this.networkMbps = networkMbps;
        return this;
    }

    /**
     * Limits the amount of disk space the task group can use to the number of MB you pass in to this method.
     *
     * @param disk the maximum amount of disk space, in MB
     * @return the same {@code ResAllocsBuilder}, modified accordingly
     */
    public ResAllocsBuilder withDisk(double disk) {
        this.disk = disk;
        return this;
    }

    /**
     * Builds a {@link ResAllocs} object based on your builder method instructions.
     *
     * @return a {@link ResAllocs} object, built to your specifications
     */
    public ResAllocs build() {
        return new ResAllocsImpl(taskGroupName, cores, memory, networkMbps, disk);
    }

    private static class ResAllocsImpl implements ResAllocs {
        private final String taskGroupName;
        private final double cores;
        private final double memory;
        private final double networkMbps;
        private final double disk;

        private ResAllocsImpl(String taskGroupName, double cores, double memory, double networkMbps, double disk) {
            this.taskGroupName = taskGroupName;
            this.cores = cores;
            this.memory = memory;
            this.networkMbps = networkMbps;
            this.disk = disk;
        }

        @Override
        public String getTaskGroupName() {
            return taskGroupName;
        }

        @Override
        public double getCores() {
            return cores;
        }

        @Override
        public double getMemory() {
            return memory;
        }

        @Override
        public double getNetworkMbps() {
            return networkMbps;
        }

        @Override
        public double getDisk() {
            return disk;
        }

        @Override
        public String toString() {
            return "ResAllocsImpl{" +
                    "taskGroupName='" + taskGroupName + '\'' +
                    ", cores=" + cores +
                    ", memory=" + memory +
                    ", networkMbps=" + networkMbps +
                    ", disk=" + disk +
                    '}';
        }
    }
}
