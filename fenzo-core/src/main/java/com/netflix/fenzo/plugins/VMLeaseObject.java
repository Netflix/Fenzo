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

import java.util.*;

/**
 * An adapter class to transform a Mesos resource offer to a Fenzo {@link VirtualMachineLease}. Pass a Mesos
 * {@link org.apache.mesos.Protos.Offer Offer} to the {@code VMLeaseObject} constructor to transform it into an
 * object that implements the {@link VirtualMachineLease} interface.
 */
public class VMLeaseObject implements VirtualMachineLease {
    private static final Logger logger = LoggerFactory.getLogger(VMLeaseObject.class);
    private final Protos.Offer offer;
    private final String hostname;
    private final String vmID;
    private final Map<String, Protos.Attribute> attributeMap;
    private final Map<String, Double> scalarResources;
    private final Map<String, List<Range>> rangeResources;
    private final long offeredTime;

    public VMLeaseObject(Protos.Offer offer) {
        this.offer = offer;
        hostname = offer.getHostname();
        this.vmID = offer.getSlaveId().getValue();
        offeredTime = System.currentTimeMillis();
        scalarResources = new HashMap<>();
        rangeResources = new HashMap<>();
        // parse out resources from offer
        // expects network bandwidth to come in as consumable scalar resource named "network"
        for (Protos.Resource resource : offer.getResourcesList()) {
            switch (resource.getType()) {
                case SCALAR:
                    scalarResources.put(resource.getName(), resource.getScalar().getValue());
                    break;
                case RANGES:
                    List<Range> ranges = new ArrayList<>();
                    for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
                        ranges.add(new Range((int)range.getBegin(), (int) range.getEnd()));
                    }
                    rangeResources.put(resource.getName(), ranges);
                    break;
                default:
                    logger.debug("Unknown resource type " + resource.getType() + " for resource " + resource.getName() +
                            " in offer, hostname=" + hostname + ", offerId=" + offer.getId());
            }
        }
        if(offer.getAttributesCount()>0) {
            Map<String, Protos.Attribute> attributeMap = new HashMap<>();
            for(Protos.Attribute attribute: offer.getAttributesList()) {
                attributeMap.put(attribute.getName(), attribute);
            }
            this.attributeMap = Collections.unmodifiableMap(attributeMap);
        } else {
            this.attributeMap = Collections.emptyMap();
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
        return scalarResources.get("cpus")==null? 0.0 : scalarResources.get("cpus");
    }
    @Override
    public double memoryMB() {
        return scalarResources.get("mem")==null? 0.0 : scalarResources.get("mem");
    }
    @Override
    public double networkMbps() {
        return scalarResources.get("network")==null? 0.0 : scalarResources.get("network");
    }
    @Override
    public double diskMB() {
        return scalarResources.get("disk")==null? 0.0 : scalarResources.get("disk");
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
        return rangeResources.get("ports")==null? Collections.<Range>emptyList() : rangeResources.get("ports");
    }
    @Override
    public Map<String, Protos.Attribute> getAttributeMap() {
        return attributeMap;
    }

    @Override
    public Double getScalarValue(String name) {
        return scalarResources.get(name);
    }

    @Override
    public Map<String, Double> getScalarValues() {
        return Collections.unmodifiableMap(scalarResources);
    }

    @Override
    public String toString() {
        return "VMLeaseObject{" +
                "offer=" + offer +
                ", scalars: " + scalarResources.toString() +
                ", ranges: " + rangeResources.toString() +
                ", hostname='" + hostname + '\'' +
                ", vmID='" + vmID + '\'' +
                ", attributeMap=" + attributeMap +
                ", offeredTime=" + offeredTime +
                '}';
    }
}
