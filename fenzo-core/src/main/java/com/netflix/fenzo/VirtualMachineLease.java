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
 * A representation of a lease (resource offer). You describe each Mesos resource offer by means of an object
 * that implements the {@code VirtualMachineLease} interface. You can get such an object by passing the
 * {@link org.apache.mesos.Protos.Offer Offer} object that represents that Mesos offer (and that you received
 * from Mesos) into the {@link com.netflix.fenzo.plugins.VMLeaseObject VMLeaseObject} constructor.
 */
public interface VirtualMachineLease {

    /**
     * Describes a [beginning, end] range.
     */
    public static class Range {
        private final int beg;
        private final int end;
        public Range(int beg, int end) {
            this.beg = beg;
            this.end = end;
        }
        /**
         * Get the beginning value of the range.
         *
         * @return the beginning value of the range
         */
        public int getBeg() {
            return beg;
        }
        /**
         * Get the end value of the range.
         *
         * @return the end value of the range
         */
        public int getEnd() {
            return end;
        }
    }

    /**
     * Get the ID of the lease (offer ID).
     *
     * @return the lease ID
     */
    public String getId();

    /**
     * Get the time that this lease (offer) was obtained.
     *
     * @return the time when this lease was obtained, in mSecs since epoch
     */
    public long getOfferedTime();

    /**
     * Get the name of the host offered in this lease.
     *
     * @return the host name
     */
    public String hostname();

    /**
     * Get the ID of the host (mesos slave ID).
     *
     * @return the host ID
     */
    public String getVMID();

    /**
     * Get the number of cores (CPUs) on this host that are available for assigning.
     *
     * @return the number of CPUs
     */
    public double cpuCores();

    /**
     * Get the amount of memory, in MBs, on this host that is available for assigning.
     *
     * @return the amount of memory, in MB
     */
    public double memoryMB();

    /**
     * Get the amount of network bandwidth, in Mbps, on this host that is available for assigning.
     *
     * @return the network bandwidth, in Mbps
     */
    public double networkMbps();

    /**
     * Get the amount of disk space, in MB, on this host that is avaialble for assigning.
     *
     * @return the amount of available disk space, in MB
     */
    public double diskMB();

    /**
     * Get the list of port ranges on this host that are available for assigning.
     *
     * @return a List of port ranges
     */
    public List<Range> portRanges();

    /**
     * Get the Mesos resource offer associated with this lease.
     *
     * @return the Mesos resource offer
     */
    public Protos.Offer getOffer();

    /**
     * Get the map of Mesos attributes associated with this lease (offer).
     *
     * @return a Map of attribute names to attribute values
     */
    public Map<String, Protos.Attribute> getAttributeMap();

    /**
     * Get the value of the scalar resource for the given <code>name</code>.
     * @param name Name of the scalar resource.
     * @return Value of the requested scalar resource if available, <code>null</code> otherwise.
     */
    Double getScalarValue(String name);

    /**
     * Get a map of all of the scalar resources with resource names as the key and resource value as the value.
     * Although cpus, memory, networkMbps, and disk are scalar resources, Fenzo currently treats them separately. Use
     * this scalar values collection to specify scalar resources other than those four.
     * @return All of the scalar resources available.
     */
    Map<String, Double> getScalarValues();
}
