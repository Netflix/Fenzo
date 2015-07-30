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
 * An adapter class to transform a Mesos resource offer to Fenzo {@link VirtualMachineLease}.
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
}
