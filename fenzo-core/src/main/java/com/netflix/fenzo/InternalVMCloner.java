/*
 * Copyright 2017 Netflix, Inc.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.mesos.Protos;

/* package */ class InternalVMCloner {

    /**
     * Creates a VM lease object representing the max of resources from the given collection of VMs. Attributes
     * are created by adding all unique attributes across the collection.
     *
     * @param avms Collection of VMs to create the new lease object from.
     * @return VM lease object representing the max of resources from the given collection of VMs
     */
    /* package */  VirtualMachineLease getClonedMaxResourcesLease(Collection<AssignableVirtualMachine> avms) {
        double cpus = 0.0;
        double mem = 0.0;
        double disk = 0.0;
        double network = 0.0;
        double ports = 0.0;
        final Map<String, Double> scalars = new HashMap<>();
        final Map<String, Protos.Attribute> attributeMap = new HashMap<>();
        if (avms != null) {
            for (AssignableVirtualMachine avm : avms) {
                Map<VMResource, Double> maxResources = avm.getMaxResources();
                Double value = maxResources.get(VMResource.CPU);
                if (value != null) {
                    cpus = Math.max(cpus, value);
                }
                value = maxResources.get(VMResource.Memory);
                if (value != null) {
                    mem = Math.max(mem, value);
                }
                value = maxResources.get(VMResource.Disk);
                if (value != null) {
                    disk = Math.max(disk, value);
                }
                value = maxResources.get(VMResource.Network);
                if (value != null) {
                    network = Math.max(network, value);
                }
                value = maxResources.get(VMResource.Ports);
                if (value != null) {
                    ports = Math.max(ports, value);
                }
                final Map<String, Double> maxScalars = avm.getMaxScalars();
                if (maxScalars != null && !maxScalars.isEmpty()) {
                    for (String k : maxScalars.keySet())
                        scalars.compute(k, (s, oldVal) -> {
                            if (oldVal == null) {
                                oldVal = 0.0;
                            }
                            Double aDouble = maxScalars.get(k);
                            if (aDouble == null) {
                                aDouble = 0.0;
                            }
                            return oldVal + aDouble;
                        });
                }
                final Map<String, Protos.Attribute> attrs = avm.getCurrTotalLease().getAttributeMap();
                if (attrs != null && !attrs.isEmpty()) {
                    for (Map.Entry<String, Protos.Attribute> e : attrs.entrySet())
                        attributeMap.putIfAbsent(e.getKey(), e.getValue());
                }
            }
        }
        final double fcpus = cpus;
        final double fmem = mem;
        final double fdisk = disk;
        final double fnetwork = network;
        final List<VirtualMachineLease.Range> fports = Collections.singletonList(
                new VirtualMachineLease.Range(100, 100 + (int) ports));
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return "NoID";
            }

            @Override
            public long getOfferedTime() {
                return 0;
            }

            @Override
            public String hostname() {
                return "NoHostname";
            }

            @Override
            public String getVMID() {
                return "NoID";
            }

            @Override
            public double cpuCores() {
                return fcpus;
            }

            @Override
            public double memoryMB() {
                return fmem;
            }

            @Override
            public double networkMbps() {
                return fnetwork;
            }

            @Override
            public double diskMB() {
                return fdisk;
            }

            @Override
            public List<Range> portRanges() {
                return fports;
            }

            @Override
            public Protos.Offer getOffer() {
                return null;
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return attributeMap;
            }

            @Override
            public Double getScalarValue(String name) {
                return scalars.get(name);
            }

            @Override
            public Map<String, Double> getScalarValues() {
                return scalars;
            }
        };
    }

    /* package */ VirtualMachineLease cloneLease(VirtualMachineLease lease, final String hostname, long now) {
        final List<VirtualMachineLease.Range> ports = new LinkedList<>();
        final List<VirtualMachineLease.Range> ranges = lease.portRanges();
        if (ranges != null && !ranges.isEmpty()) {
            for (VirtualMachineLease.Range r : ranges)
                ports.add(new VirtualMachineLease.Range(r.getBeg(), r.getEnd()));
        }
        final double cpus = lease.cpuCores();
        final double memory = lease.memoryMB();
        final double network = lease.networkMbps();
        final double disk = lease.diskMB();
        final Map<String, Protos.Attribute> attributes = new HashMap<>(lease.getAttributeMap());
        final Map<String, Double> scalarValues = new HashMap<>(lease.getScalarValues());
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return hostname;
            }

            @Override
            public long getOfferedTime() {
                return now;
            }

            @Override
            public String hostname() {
                return hostname;
            }

            @Override
            public String getVMID() {
                return hostname;
            }

            @Override
            public double cpuCores() {
                return cpus;
            }

            @Override
            public double memoryMB() {
                return memory;
            }

            @Override
            public double networkMbps() {
                return network;
            }

            @Override
            public double diskMB() {
                return disk;
            }

            @Override
            public List<Range> portRanges() {
                return ports;
            }

            @Override
            public Protos.Offer getOffer() {
                return null;
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return attributes;
            }

            @Override
            public Double getScalarValue(String name) {
                return scalarValues.get(name);
            }

            @Override
            public Map<String, Double> getScalarValues() {
                return scalarValues;
            }
        };
    }

}
