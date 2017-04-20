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
package com.netflix.fenzo.plugins;

import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestableVirtualMachineLease implements VirtualMachineLease {

    private final String id;

    private TestableVirtualMachineLease(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public long getOfferedTime() {
        return 0;
    }

    @Override
    public String hostname() {
        return null;
    }

    @Override
    public String getVMID() {
        return null;
    }

    @Override
    public double cpuCores() {
        return 0;
    }

    @Override
    public double memoryMB() {
        return 0;
    }

    @Override
    public double networkMbps() {
        return 0;
    }

    @Override
    public double diskMB() {
        return 0;
    }

    @Override
    public List<Range> portRanges() {
        return null;
    }

    @Override
    public Protos.Offer getOffer() {
        return null;
    }

    @Override
    public Map<String, Protos.Attribute> getAttributeMap() {
        return null;
    }

    @Override
    public Double getScalarValue(String name) {
        return null;
    }

    @Override
    public Map<String, Double> getScalarValues() {
        return null;
    }

    public static TestableVirtualMachineLease leaseWithId(String id) {
        return new TestableVirtualMachineLease(id);
    }

    public static List<VirtualMachineLease> leasesWithIds(String... ids) {
        List<VirtualMachineLease> leases = new ArrayList<>(ids.length);
        for (String id : ids) {
            leases.add(new TestableVirtualMachineLease(id));
        }
        return leases;
    }
}
