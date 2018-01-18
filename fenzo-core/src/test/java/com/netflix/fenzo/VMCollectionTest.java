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
    private final Map<String, Protos.Attribute> activeVmAttribute1 = new HashMap<>();
    private final Map<String, Protos.Attribute> activeVmAttribute2 = new HashMap<>();
    private final List<VirtualMachineLease.Range> ports = new ArrayList<>();
    private final String attributeVal1 = "4cores";
    private final String vmAttrVal1 = "val1";
    private final String vmAttrVal2 = "val2";
    private final String activeAttr = "Asg";

    @Before
    public void setUp() throws Exception {
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(activeAttr)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(attributeVal1)).build();
        attributes1.put(AutoScalerTest.hostAttrName, attribute);
        Protos.Attribute vmAttr1 = Protos.Attribute.newBuilder().setName(activeAttr)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(vmAttrVal1)).build();
        activeVmAttribute1.put(activeAttr, vmAttr1);
        activeVmAttribute1.put(AutoScalerTest.hostAttrName, attribute);
        Protos.Attribute vmAttr2 = Protos.Attribute.newBuilder().setName(activeAttr)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(vmAttrVal2)).build();
        activeVmAttribute2.put(activeAttr, vmAttr2);
        activeVmAttribute2.put(AutoScalerTest.hostAttrName, attribute);
        ports.add(new VirtualMachineLease.Range(1, 100));
    }

    @Test
    public void clonePseudoVMsForGroups() throws Exception {
        AutoScaleRule rule = getAutoScaleRule(attributeVal1, 4);
        Action1<VirtualMachineLease> leaseRejectAction = l -> {};
        final ConcurrentMap<String, String> vmIdTohostNames = new ConcurrentHashMap<>();
        final ConcurrentMap<String, String> leasesToHostnames = new ConcurrentHashMap<>();
        final Map<String, AssignableVirtualMachine> avms = new HashMap<>();
        final TaskTracker taskTracker = new TaskTracker();
        VMCollection vms = createVmCollection(vmIdTohostNames, leasesToHostnames, taskTracker, avms);
        for (int i=0; i < rule.getMaxSize()-1; i++) {
            vms.addLease(LeaseProvider.getLeaseOffer("host"+i, 4, 4000,
                    0, 0, ports, attributes1, Collections.singletonMap("GPU", 1.0)));
        }
        for (AssignableVirtualMachine avm: avms.values())
            avm.updateCurrTotalLease();
        final Map<String, List<String>> map = vms.clonePseudoVMsForGroups(
                Collections.singletonMap(rule.getRuleName(), 6), s -> rule,
                lease -> true
        );
        Assert.assertNotNull(map);
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(rule.getRuleName(), map.keySet().iterator().next());
        Assert.assertEquals(1, map.values().iterator().next().size());
    }

    @Test
    public void testWithInactiveAgents() throws Exception {
        AutoScaleRule rule = getAutoScaleRule(attributeVal1, 4);
        final ConcurrentMap<String, String> vmIdTohostNames = new ConcurrentHashMap<>();
        final ConcurrentMap<String, String> leasesToHostnames = new ConcurrentHashMap<>();
        final Map<String, AssignableVirtualMachine> avms = new HashMap<>();
        TaskTracker taskTracker = new TaskTracker();
        VMCollection vms = createVmCollection(vmIdTohostNames, leasesToHostnames, taskTracker, avms);
        // create leases for ruleMax-2 VMs for each of vmAttrVal1 and vmAttrVal2
        for (int i=0; i<rule.getMaxSize()-2; i++) {
            vms.addLease(LeaseProvider.getLeaseOffer("host-a1-"+i, 4, 4000, 0, 0,
                    ports, activeVmAttribute1, Collections.singletonMap("GPU", 1.0)));
            vms.addLease(LeaseProvider.getLeaseOffer("host-a2-"+i, 4, 4000, 0, 0,
                    ports, activeVmAttribute2, Collections.singletonMap("GPU", 1.0)));
        }
        for (AssignableVirtualMachine avm: avms.values())
            avm.updateCurrTotalLease();
        final Map<String, List<String>> map = vms.clonePseudoVMsForGroups(
                Collections.singletonMap(rule.getRuleName(), 7), s -> rule,
                lease -> {
                    System.out.println("Comparing " + vmAttrVal2 + " with " + lease.getAttributeMap().get(activeAttr).getText().getValue());
                    return vmAttrVal2.equals(lease.getAttributeMap().get(activeAttr).getText().getValue());
                }
        );
        Assert.assertNotNull(map);
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(2, map.values().iterator().next().size());
        for (String h: map.values().iterator().next()) {
            final AssignableVirtualMachine avm = avms.get(h);
            Assert.assertNotNull(avm);
            avm.updateCurrTotalLease();
            final String attrValue = avm.getAttrValue(activeAttr);
            Assert.assertTrue("Invalid to get host " + h + " with attrVal " + attrValue,
                    vmAttrVal2.equals(attrValue));
        }
    }

    private VMCollection createVmCollection(ConcurrentMap<String, String> vmIdTohostNames,
                                            ConcurrentMap<String, String> leasesToHostnames, TaskTracker taskTracker,
                                            Map<String, AssignableVirtualMachine> avms) {
        Action1<VirtualMachineLease> leaseRejectAction = l -> {};
        return new VMCollection(s -> {
            AssignableVirtualMachine avm = new AssignableVirtualMachine(
                    DefaultPreferentialNamedConsumableResourceEvaluator.INSTANCE,
                    vmIdTohostNames,
                    leasesToHostnames,
                    s,
                    leaseRejectAction,
                    2,
                    taskTracker,
                    false
            );
            avms.put(s, avm);
            return avm;
        }, AutoScalerTest.hostAttrName);
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