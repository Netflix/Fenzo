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

import java.util.*;

public class LeaseProvider {

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus,
                                             final double memory, final List<VirtualMachineLease.Range> portRanges) {
        return getLeaseOffer(hostname, cpus, memory, 0.0, portRanges, null);
    }

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus, final double memory,
                                             final double network, final List<VirtualMachineLease.Range> portRanges) {
        return getLeaseOffer(hostname, cpus, memory, network, portRanges, null);
    }

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus, final double memory,
                                             final List<VirtualMachineLease.Range> portRanges,
                                             final Map<String, Protos.Attribute> attributesMap) {
        return getLeaseOffer(hostname, cpus, memory, 0.0, portRanges, attributesMap);
    }

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus, final double memory, final double disk,
                                             final double network, final List<VirtualMachineLease.Range> portRanges,
                                             final Map<String, Protos.Attribute> attributesMap) {
        return getLeaseOffer(hostname, cpus, memory, disk, network, portRanges, attributesMap, null);
    }

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus, final double memory, final double disk,
                                             final double network, final List<VirtualMachineLease.Range> portRanges,
                                             final Map<String, Protos.Attribute> attributesMap, Map<String, Double> scalarResources) {
        final long offeredTime = System.currentTimeMillis();
        final String id = UUID.randomUUID().toString();
        final String vmId = UUID.randomUUID().toString();
        final Map<String, Double> scalars = scalarResources==null? Collections.<String, Double>emptyMap() : scalarResources;
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return id;
            }
            @Override
            public long getOfferedTime() {
                return offeredTime;
            }
            @Override
            public String hostname() {
                return hostname;
            }
            @Override
            public String getVMID() {
                return vmId;
            }
            @Override
            public double cpuCores() {
                return cpus;
            }
            @Override
            public double memoryMB() {
                return memory;
            }
            public double networkMbps() {
                return network;
            }
            @Override
            public double diskMB() {
                return disk;
            }
            @Override
            public List<Range> portRanges() {
                return portRanges;
            }
            @Override
            public Protos.Offer getOffer() {
                return Protos.Offer.getDefaultInstance()
                        .toBuilder()
                        .setId(Protos.OfferID.getDefaultInstance().toBuilder().setValue(id).build())
                        .setHostname(hostname)
                        .setSlaveId(Protos.SlaveID.getDefaultInstance().toBuilder().setValue(vmId).build())
                        .setFrameworkId(Protos.FrameworkID.getDefaultInstance().toBuilder().setValue("Testing").build())
                        .build();
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return attributesMap==null? null : attributesMap;
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

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus, final double memory,
                                             final double network, final List<VirtualMachineLease.Range> portRanges,
                                             final Map<String, Protos.Attribute> attributesMap) {
        return getLeaseOffer(hostname, cpus, memory, 500000, network, portRanges, attributesMap);
    }

    public static VirtualMachineLease getLeaseOffer(final String hostname, final double cpus,
                                             final double memory, final int portBegin, final int portEnd) {
        List<VirtualMachineLease.Range> ranges = new ArrayList<>(1);
        ranges.add(new VirtualMachineLease.Range(portBegin, portEnd));
        return getLeaseOffer(hostname, cpus, memory, ranges);
    }

    public static List<VirtualMachineLease> getLeases(int numHosts, double cpus, double memory,
                                                int portBeg, int portEnd) {
        return getLeases(numHosts, cpus, memory, 0.0, portBeg, portEnd);
    }
    public static List<VirtualMachineLease> getLeases(int numHosts, double cpus, double memory, double network,
                                               int portBeg, int portEnd) {
        return getLeases(0, numHosts, cpus, memory, network, portBeg, portEnd);
    }
    public static List<VirtualMachineLease> getLeases(int hostSuffixBegin, int numHosts, double cpus, double memory,
                                               int portBeg, int portEnd) {
        return getLeases(hostSuffixBegin, numHosts, cpus, memory, 0.0, portBeg, portEnd);
    }
    public static List<VirtualMachineLease> getLeases(int hostSuffixBegin, int numHosts, double cpus, double memory,
                                               double network, int portBeg, int portEnd) {
        VirtualMachineLease.Range range = new VirtualMachineLease.Range(portBeg, portEnd);
        List<VirtualMachineLease.Range> ranges = new ArrayList<>(1);
        ranges.add(range);
        return getLeases(hostSuffixBegin, numHosts, cpus, memory, network, ranges);
    }

    public static List<VirtualMachineLease> getLeases(int numHosts, double cpus, double memory,
                                                List<VirtualMachineLease.Range> ports) {
        return getLeases(0, numHosts, cpus, memory, ports);
    }
    public static List<VirtualMachineLease> getLeases(int hostSuffixBegin, int numHosts, double cpus, double memory,
                                               List<VirtualMachineLease.Range> ports) {
        return getLeases(hostSuffixBegin, numHosts, cpus, memory, 0.0, ports);
    }
    public static List<VirtualMachineLease> getLeases(int hostSuffixBegin, int numHosts, double cpus, double memory,
                                               double network, List<VirtualMachineLease.Range> ports) {
        List<VirtualMachineLease> leases = new ArrayList<>(numHosts);
        for(int i=hostSuffixBegin; i<(hostSuffixBegin+numHosts); i++)
            leases.add(getLeaseOffer("host"+i, cpus, memory, network, ports));
        return leases;
    }

    private static List<VirtualMachineLease.Range> getRangesAfterConsuming(List<VirtualMachineLease.Range> orig, int consumePort) {
        List<VirtualMachineLease.Range> result = new ArrayList<>();
        for(int i=0; i<orig.size(); i++) {
            VirtualMachineLease.Range range = orig.get(i);
            if(consumePort<range.getBeg() || consumePort>range.getEnd()) {
                result.add(range);
                continue;
            }
            if(range.getBeg() != range.getEnd()) {
                if(consumePort>range.getBeg()) {
                    VirtualMachineLease.Range split = new VirtualMachineLease.Range(range.getBeg(), consumePort-1);
                    result.add(split);
                }
                if(consumePort<range.getEnd())
                    result.add(new VirtualMachineLease.Range(consumePort+1, range.getEnd()));
            }
            for(int j=i+1; j<orig.size(); j++)
                result.add(orig.get(j));
            return result;
        }
        throw new IllegalArgumentException("Unexpected to not find " + consumePort + " within the ranges provided");
    }

    public static VirtualMachineLease getConsumedLease(VMAssignmentResult result) {
        double cpus=0.0;
        double memory=0.0;
        double network = 0.0;
        List<Integer> ports = new ArrayList<>();
        for(TaskAssignmentResult r: result.getTasksAssigned()) {
            cpus += r.getRequest().getCPUs();
            memory += r.getRequest().getMemory();
            network += r.getRequest().getNetworkMbps();
            ports.addAll(r.getAssignedPorts());
        }
        double totalCpus=0.0;
        double totalMem=0.0;
        double totalNetwork=0.0;
        List<VirtualMachineLease.Range> totPortRanges = new ArrayList<>();
        String hostname="";
        Map<String, Protos.Attribute> attributes = null;
        for(VirtualMachineLease l: result.getLeasesUsed()) {
            hostname = l.hostname();
            attributes = l.getAttributeMap();
            totalCpus += l.cpuCores();
            totalMem += l.memoryMB();
            totalNetwork += l.networkMbps();
            totPortRanges.addAll(l.portRanges());
        }
        for(Integer port: ports)
            totPortRanges = getRangesAfterConsuming(totPortRanges, port);
        return getLeaseOffer(hostname, totalCpus-cpus, totalMem-memory, totalNetwork-network, totPortRanges, attributes);
    }

    public static VirtualMachineLease getConsumedLease(VirtualMachineLease orig, double consumedCpu, double consumedMemory, List<Integer> consumedPorts) {
        final double cpu = orig.cpuCores() - consumedCpu;
        final double memory = orig.memoryMB() - consumedMemory;
        List<VirtualMachineLease.Range> ranges = orig.portRanges();
        for(Integer port: consumedPorts) {
            ranges = getRangesAfterConsuming(ranges, port);
        }
        return getLeaseOffer(orig.hostname(), cpu, memory, 0.0, ranges, orig.getAttributeMap());
    }
}
