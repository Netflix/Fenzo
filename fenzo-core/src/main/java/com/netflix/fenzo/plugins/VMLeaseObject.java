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

package com.netflix.fenzo.plugins;

import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An adapter class to transform a Mesos resource offer to a Fenzo {@link VirtualMachineLease}. Pass a Mesos
 * {@link org.apache.mesos.Protos.Offer Offer} to the {@code VMLeaseObject} constructor to transform it into an
 * object that implements the {@link VirtualMachineLease} interface.
 */
public class VMLeaseObject implements VirtualMachineLease {
    private static final Logger logger = LoggerFactory.getLogger(VMLeaseObject.class);
    private final Protos.Offer offer;
    private double cpuCores;
    private double memoryMB;
    private double networkMbps=0.0;
    private double diskMB;
    private final String hostname;
    private final String vmID;
    private List<Range> portRanges;
    private final Map<String, Protos.Attribute> attributeMap;
    private final long offeredTime;

    public VMLeaseObject(Protos.Offer offer) {
        this.offer = offer;
        hostname = offer.getHostname();
        this.vmID = offer.getSlaveId().getValue();
        offeredTime = System.currentTimeMillis();
        // parse out resources from offer
        // expects network bandwidth to come in as consumable scalar resource named "network"
        for (Protos.Resource resource : offer.getResourcesList()) {
            switch (resource.getName()) {
                case "cpus":
                    cpuCores = resource.getScalar().getValue();
                    break;
                case "mem":
                    memoryMB = resource.getScalar().getValue();
                    break;
                case "network":
                    networkMbps = resource.getScalar().getValue();
                    break;
                case "disk":
                    diskMB = resource.getScalar().getValue();
                    break;
                case "ports":
                    portRanges = new ArrayList<>();
                    for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
                        portRanges.add(new Range((int)range.getBegin(), (int) range.getEnd()));
                    }
                    break;
                default:
                    logger.debug("Unknown resource " + resource.getName() + " in offer, hostname=" + hostname +
                            ", offerId=" + offer.getId());
            }
        }
        attributeMap = new HashMap<>();
        if(offer.getAttributesCount()>0) {
            for(Protos.Attribute attribute: offer.getAttributesList()) {
                attributeMap.put(attribute.getName(), attribute);
            }
        }
    }
    @Override
    public String hostname() {
        return hostname;
    }
    @Override
    public String getVMID() {
        return vmID;
    }
    @Override
    public double cpuCores() {
        return cpuCores;
    }
    @Override
    public double memoryMB() {
        return memoryMB;
    }
    @Override
    public double networkMbps() {
        return networkMbps;
    }
    @Override
    public double diskMB() {
        return diskMB;
    }
    public Protos.Offer getOffer(){
        return offer;
    }
    @Override
    public String getId() {
        return offer.getId().getValue();
    }
    @Override
    public long getOfferedTime() {
        return offeredTime;
    }
    @Override
    public List<Range> portRanges() {
        return portRanges;
    }
    @Override
    public Map<String, Protos.Attribute> getAttributeMap() {
        return attributeMap;
    }

    @Override
    public String toString() {
        return "VMLeaseObject{" +
                "offer=" + offer +
                ", cpuCores=" + cpuCores +
                ", memoryMB=" + memoryMB +
                ", networkMbps=" + networkMbps +
                ", diskMB=" + diskMB +
                ", hostname='" + hostname + '\'' +
                ", vmID='" + vmID + '\'' +
                ", portRanges=" + portRanges +
                ", attributeMap=" + attributeMap +
                ", offeredTime=" + offeredTime +
                '}';
    }
}
