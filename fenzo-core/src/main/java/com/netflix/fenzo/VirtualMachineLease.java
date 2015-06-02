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
    public String getVMID();
	public double cpuCores();
	public double memoryMB();
    public double networkMbps();
	public double diskMB();
	public List<Range> portRanges();
	public Protos.Offer getOffer();
    public Map<String, Protos.Attribute> getAttributeMap();
}
