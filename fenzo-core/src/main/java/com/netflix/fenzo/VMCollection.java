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

import com.netflix.fenzo.functions.Func1;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class VMCollection {
    private static final String defaultGroupName = "DEFAULT";
    private static final Logger logger = LoggerFactory.getLogger(VMCollection.class);
    private final ConcurrentMap<String, ConcurrentMap<String, AssignableVirtualMachine>> vms;
    private final Func1<String, AssignableVirtualMachine> newVmCreator;
    private final String groupingAttrName;

    VMCollection(Func1<String, AssignableVirtualMachine> func1, String groupingAttrName) {
        vms = new ConcurrentHashMap<>();
        this.newVmCreator = func1;
        this.groupingAttrName = groupingAttrName;
    }

    Collection<AssignableVirtualMachine> getAllVMs() {
        List<AssignableVirtualMachine> result = new LinkedList<>();
        vms.values().forEach(m -> result.addAll(m.values()));
        return result;
    }

    Collection<String> getGroups() {
        return Collections.unmodifiableCollection(vms.keySet());
    }

    /**
     * Create <code>n</code> pseudo VMs for each group by cloning a VM in each group.
     * @param groupCounts Map with keys contain group names and values containing number of agents to clone
     * @param ruleGetter Getter function for autoscale rules
     * @return Collection of pseudo host names added.
     */
    Map<String, List<String>> clonePseudoVMsForGroups(Map<String, Integer> groupCounts,
                                                      Func1<String, AutoScaleRule> ruleGetter,
                                                      Predicate<VirtualMachineLease> vmFilter
    ) {
        if (groupCounts == null || groupCounts.isEmpty())
            return Collections.emptyMap();
        InternalVMCloner vmCloner = new InternalVMCloner();
        Map<String, List<String>> result = new HashMap<>();
        long now = System.currentTimeMillis();
        for (String g: groupCounts.keySet()) {
            List<String> hostnames = new LinkedList<>();
            result.put(g, hostnames);
            final ConcurrentMap<String, AssignableVirtualMachine> avmsMap = vms.get(g);
            if (avmsMap != null) {
                final List<AssignableVirtualMachine> vmsList = avmsMap.values()
                        .stream()
                        .filter(avm -> vmFilter.test(avm.getCurrTotalLease()))
                        .collect(Collectors.toList());
                if (vmsList != null && !vmsList.isEmpty()) {
                    // NOTE: a shortcoming here is that the attributes of VMs across a group may not be homogeneous.
                    // By creating one lease object and cloning from it, we pick one combination of the attributes
                    // and replicate across all the newly created pseudo VMs. It may be possible to capture the
                    // unique set of attributes across all existing VMs in the group and replicate that mix within
                    // the new pseudo VMs. However, we will not do that at this time. So, it is possible that some
                    // task constraints that depend on the variety of such attributes may fail the task placement.
                    // We will live with that limitation at this time.
                    VirtualMachineLease lease = vmCloner.getClonedMaxResourcesLease(vmsList);
                    if(logger.isDebugEnabled()) {
                        logger.debug("Cloned lease cpu={}, mem={}, disk={}, network={}", lease.cpuCores(),
                                lease.memoryMB(), lease.diskMB(), lease.networkMbps());
                        final Map<String, Protos.Attribute> attributeMap = lease.getAttributeMap();
                        if (attributeMap == null || attributeMap.isEmpty())
                            logger.debug("Cloned maxRes lease has empty attributeMap");
                        else
                            for (Map.Entry<String, Protos.Attribute> entry : attributeMap.entrySet())
                                logger.debug("Cloned maxRes lease attribute: " + entry.getKey() + ": " +
                                        (entry.getValue() == null ? "null" : entry.getValue().getText().getValue())
                                );
                    }
                    int n = groupCounts.get(g);
                    final AutoScaleRule rule = ruleGetter.call(g);
                    if (rule != null) {
                        int max = rule.getMaxSize();
                        if (max < Integer.MAX_VALUE && n > (max - vmsList.size()))
                            n = max - vmsList.size();
                    }
                    for (int i = 0; i < n; i++) {
                        final String hostname = createHostname(g, i);
                        addLease(vmCloner.cloneLease(lease, hostname, now));
                        if(logger.isDebugEnabled())
                            logger.debug("Added cloned lease for " + hostname);
                        hostnames.add(hostname);
                        // update total lease on the newly added VMs so they are available for use
                        getVmByName(hostname).ifPresent(AssignableVirtualMachine::updateCurrTotalLease);
                    }
                }
            }
        }
        return result;
    }

    private String createHostname(String g, int i) {
        return AssignableVirtualMachine.PseuoHostNamePrefix + g + "-" + i;
    }

    /**
     * Remove VM of given name from the given group. This is generally unsafe and intended only to be used by whoever
     * uses {@link VMCollection#clonePseudoVMsForGroups(Map, Func1, Predicate)}.
     * @param name
     * @param group
     */
    /* package */ AssignableVirtualMachine unsafeRemoveVm(String name, String group) {
        final ConcurrentMap<String, AssignableVirtualMachine> vmsMap = vms.get(group);
        if (vmsMap != null) {
            return vmsMap.remove(name);
        }
        return null;
    }

    Optional<AssignableVirtualMachine> getVmByName(String name) {
        return vms.values().stream()
                .flatMap(
                        (Function<ConcurrentMap<String, AssignableVirtualMachine>, Stream<AssignableVirtualMachine>>) m
                                -> m.values().stream()
                )
                .filter(avm -> name.equals(avm.getHostname()))
                .findFirst();
    }

    AssignableVirtualMachine create(String host) {
        return create(host, defaultGroupName);
    }

    AssignableVirtualMachine create(String host, String group) {
        vms.putIfAbsent(group, new ConcurrentHashMap<>());
        AssignableVirtualMachine prev = null;
        if (!defaultGroupName.equals(group)) {
            if (vms.get(defaultGroupName) != null)
                prev = vms.get(defaultGroupName).remove(host);
        }
        vms.get(group).putIfAbsent(host, prev == null? newVmCreator.call(host) : prev);
        return vms.get(group).get(host);
    }

    AssignableVirtualMachine getOrCreate(String host) {
        final Optional<AssignableVirtualMachine> vmByName = getVmByName(host);
        if (vmByName.isPresent())
            return vmByName.get();
        return create(host, defaultGroupName);
    }

    private AssignableVirtualMachine getOrCreate(String host, String group) {
        vms.putIfAbsent(group, new ConcurrentHashMap<>());
        final AssignableVirtualMachine avm = vms.get(group).get(host);
        if (avm != null)
            return avm;
        if (logger.isDebugEnabled())
            logger.debug("Creating new host " + host);
        return create(host, group);
    }

    boolean addLease(VirtualMachineLease l) {
        String group = l.getAttributeMap() == null? null :
                l.getAttributeMap().get(groupingAttrName) == null?
                        null :
                        l.getAttributeMap().get(groupingAttrName).getText().getValue();
        if (group == null)
            group = defaultGroupName;
        final AssignableVirtualMachine avm = getOrCreate(l.hostname(), group);
        return avm.addLease(l);
    }

    public int size() {
        final Optional<Integer> size = vms.values().stream().map(Map::size).reduce((i1, i2) -> i1 + i2);
        return size.orElse(0);
    }

    public int size(String group) {
        final ConcurrentMap<String, AssignableVirtualMachine> m = vms.get(group);
        return m == null? 0 : m.size();
    }

    public AssignableVirtualMachine remove(AssignableVirtualMachine avm) {
        final String group = avm.getAttrValue(groupingAttrName);
        AssignableVirtualMachine removed = null;
        if (group != null) {
            final ConcurrentMap<String, AssignableVirtualMachine> m = vms.get(group);
            if (m != null) {
                removed = m.remove(avm.getHostname());
            }
        }
        if (removed != null)
            return removed;
        final ConcurrentMap<String, AssignableVirtualMachine> m = vms.get(defaultGroupName);
        if (m != null)
            m.remove(avm.getHostname());
        return null;
    }
}
