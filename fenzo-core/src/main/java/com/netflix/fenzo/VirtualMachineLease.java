package com.netflix.fenzo;

import org.apache.mesos.Protos;

import java.util.List;
import java.util.Map;

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

    public String getId();
    public long getOfferedTime();
	public String hostname();
    public String getSlaveID();
	public double cpuCores();
	public double memoryMB();
	public double diskMB();
	public List<Range> portRanges();
	public Protos.Offer getOffer();
    public Map<String, Protos.Attribute> getAttributeMap();
}
