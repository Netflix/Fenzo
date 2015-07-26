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

import org.apache.mesos.Protos;

import java.util.List;
import java.util.Map;

/**
 * A representation of a lease (resource offer).
 */
public interface VirtualMachineLease {

    public static class Range {
        private final int beg;
        private final int end;
        public Range(int beg, int end) {
            this.beg = beg;
            this.end = end;
        }
        public int getBeg() {
            return beg;
        }
        public int getEnd() {
            return end;
        }
    }

    /**
     * Get the ID of the lease (offer ID).
     *
     * @return Lease ID.
     */
    public String getId();

    /**
     * Get the time that this lease (offer) was obtained.
     *
     * @return Time when this lease was obtained, in mSecs since epoch.
     */
    public long getOfferedTime();

    /**
     * Get the host name.
     *
     * @return Host name.
     */
    public String hostname();

    /**
     * Get the ID of the host (mesos slave ID).
     *
     * @return host ID.
     */
    public String getVMID();

    /**
     * Get the number of cores (CPUs) on this host that are available for assigning.
     *
     * @return The number of CPUs.
     */
    public double cpuCores();

    /**
     * Get the amount of memory, in MBs, on this host that are available for assigning.
     *
     * @return Amount of memory in MB.
     */
    public double memoryMB();

    /**
     * Get the amount of network bandwidth, in Mbps, on this host that is available for assigning.
     *
     * @return Network bandwidth, in Mbps.
     */
    public double networkMbps();

    /**
     * Get the amount of disk, in MB, on this host that is avaialble for assigning.
     *
     * @return The amount of disk, in MB.
     */
    public double diskMB();

    /**
     * Get the list of port ranges on this host that are available for assigning.
     *
     * @return List of port ranges.
     */
    public List<Range> portRanges();

    /**
     * Get the mesos resource offer associated with this lease.
     *
     * @return The offer.
     */
    public Protos.Offer getOffer();

    /**
     * Get the map of mesos attributes associated with this lease (offer).
     *
     * @return Map of attributes.
     */
    public Map<String, Protos.Attribute> getAttributeMap();
}
