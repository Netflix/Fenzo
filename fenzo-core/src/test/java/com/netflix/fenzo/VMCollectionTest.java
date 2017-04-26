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

import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;

public class VMCollectionTest {

    private final Map<String, Protos.Attribute> attributes1 = new HashMap<>();
    private final List<VirtualMachineLease.Range> ports = new ArrayList<>();
    private final String attributeVal1 = "4cores";

    @Before
    public void setUp() throws Exception {
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(AutoScalerTest.hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(attributeVal1)).build();
        attributes1.put(AutoScalerTest.hostAttrName, attribute);
        ports.add(new VirtualMachineLease.Range(1, 100));
    }

    @Test
    public void clonePseudoVMsForGroups() throws Exception {
        AutoScaleRule rule = getAutoScaleRule(attributeVal1, 4);
        Action1<VirtualMachineLease> leaseRejectAction = l -> {};
        final ConcurrentMap<String, String> vmIdTohostNames = new ConcurrentHashMap<>();
        final ConcurrentMap<String, String> leasesToHostnames = new ConcurrentHashMap<>();
        final Map<String, AssignableVirtualMachine> avms = new HashMap<>();
        VMCollection vms = new VMCollection(s -> {
            AssignableVirtualMachine avm = new AssignableVirtualMachine(
                    vmIdTohostNames,
                    leasesToHostnames,
                    s,
                    leaseRejectAction,
                    2,
                    new TaskTracker(),
                    false
            );
            avms.put(s, avm);
            return avm;
        }, AutoScalerTest.hostAttrName);
        for (int i=0; i < rule.getMaxSize()-1; i++) {
            vms.addLease(LeaseProvider.getLeaseOffer("host"+i, 4, 4000,
                    0, 0, ports, attributes1, Collections.singletonMap("GPU", 1.0)));
        }
        for (AssignableVirtualMachine avm: avms.values())
            avm.updateCurrTotalLease();
        final Map<String, List<String>> map = vms.clonePseudoVMsForGroups(Collections.singletonMap(rule.getRuleName(), 6), s -> rule);
        Assert.assertNotNull(map);
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(rule.getRuleName(), map.keySet().iterator().next());
        Assert.assertEquals(1, map.values().iterator().next().size());
    }

    private AutoScaleRule getAutoScaleRule(final String name, final int maxSize) {
        return new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return name;
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return 0;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return 0;
            }

            @Override
            public int getMaxSize() {
                return maxSize;
            }

            @Override
            public long getCoolDownSecs() {
                return 1;
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return false;
            }
        };
    }

}