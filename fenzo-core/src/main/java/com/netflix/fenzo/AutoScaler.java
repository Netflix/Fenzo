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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.queues.QueuableTask;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AutoScaler {

    private static final Logger logger = LoggerFactory.getLogger(AutoScaler.class);
    final VMCollection vmCollection;
    private final String mapHostnameAttributeName;
    private final String scaleDownBalancedByAttributeName;
    private final ActiveVmGroups activeVmGroups;
    private final AutoScaleRules autoScaleRules;
    private final boolean disableShortfallEvaluation;
    private final String attributeName;
    private final AssignableVMs assignableVMs;
    private final ExecutorService executor =
            new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10),
                    new ThreadPoolExecutor.DiscardPolicy());
    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final ConcurrentMap<String, ScalingActivity> scalingActivityMap = new ConcurrentHashMap<>();
    private final ScaleDownConstraintExecutor scaleDownConstraintExecutor;
    private volatile Action1<AutoScaleAction> callback = null;
    private ShortfallEvaluator shortfallEvaluator;
    private long delayScaleUpBySecs = 0L;
    private long delayScaleDownBySecs = 0L;
    private long disabledVmDurationInSecs = 0L;
    private volatile Func1<QueuableTask, List<String>> taskToClustersGetter = null;

    AutoScaler(final String attributeName, String mapHostnameAttributeName, String scaleDownBalancedByAttributeName,
               final List<AutoScaleRule> autoScaleRules, final AssignableVMs assignableVMs,
               final boolean disableShortfallEvaluation, ActiveVmGroups activeVmGroups, VMCollection vmCollection,
               ScaleDownConstraintExecutor scaleDownConstraintExecutor) {
        this.mapHostnameAttributeName = mapHostnameAttributeName;
        this.scaleDownBalancedByAttributeName = scaleDownBalancedByAttributeName;
        this.shortfallEvaluator = new NaiveShortfallEvaluator();
        this.attributeName = attributeName;
        this.autoScaleRules = new AutoScaleRules(autoScaleRules);
        this.assignableVMs = assignableVMs;
        this.disableShortfallEvaluation = disableShortfallEvaluation;
        this.activeVmGroups = activeVmGroups;
        this.vmCollection = vmCollection;
        this.scaleDownConstraintExecutor = scaleDownConstraintExecutor;
    }

    /* package */ void useOptimizingShortfallAnalyzer() {
        shortfallEvaluator = new OptimizingShortfallEvaluator();
    }

    /* package */ void setSchedulingService(TaskSchedulingService service) {
        shortfallEvaluator.setTaskSchedulingService(service);
    }

    Collection<AutoScaleRule> getRules() {
        return Collections.unmodifiableCollection(autoScaleRules.getRules());
    }

    void replaceRule(AutoScaleRule rule) {
        if (rule == null)
            throw new NullPointerException("Can't add null rule");
        autoScaleRules.replaceRule(rule);
    }

    AutoScaleRule getRule(String name) {
        return autoScaleRules.get(name);
    }

    void removeRule(String ruleName) {
        if (ruleName != null)
            autoScaleRules.remRule(ruleName);
    }

    void setDelayScaleUpBySecs(long secs) {
        delayScaleUpBySecs = secs;
    }

    void setDelayScaleDownBySecs(long secs) {
        delayScaleDownBySecs = secs;
    }

    void setDisabledVmDurationInSecs(long disabledVmDurationInSecs) {
        this.disabledVmDurationInSecs = disabledVmDurationInSecs;
    }

    void setTaskToClustersGetter(Func1<QueuableTask, List<String>> getter) {
        this.taskToClustersGetter = getter;
    }

    void doAutoscale(final AutoScalerInput autoScalerInput) {
        if (isShutdown.get()) {
            return;
        }

        try {
            shortfallEvaluator.setTaskToClustersGetter(taskToClustersGetter);
            autoScaleRules.prepare();
            Map<String, HostAttributeGroup> hostAttributeGroupMap = setupHostAttributeGroupMap(autoScaleRules, scalingActivityMap);
            if (!disableShortfallEvaluation) {
                Map<String, Integer> shortfall = shortfallEvaluator.getShortfall(hostAttributeGroupMap.keySet(), autoScalerInput.getFailures(), autoScaleRules);
                for (Map.Entry<String, Integer> entry : shortfall.entrySet()) {
                    hostAttributeGroupMap.get(entry.getKey()).shortFall = entry.getValue() == null ? 0 : entry.getValue();
                }
            }
            populateIdleResources(autoScalerInput.getIdleResourcesList(), autoScalerInput.getIdleInactiveResourceList(), hostAttributeGroupMap);
            List<Runnable> callbacks = new ArrayList<>();
            for (HostAttributeGroup hostAttributeGroup : hostAttributeGroupMap.values()) {
                callbacks.addAll(processScalingNeeds(hostAttributeGroup, scalingActivityMap, assignableVMs));
            }

            executor.submit(() -> {
                if (isShutdown.get()) {
                    return;
                }
                //Since the agents are disabled synchronously for each attribute group, the agents will become enabled again
                //if the sum duration of all callback calls takes longer than the cooldown period
                for (Runnable callback : callbacks) {
                    callback.run();
                }
            });
        } catch (Exception e) {
            logger.error("Autoscaler failure: ", e);
        }
    }

    private boolean shouldScaleNow(boolean scaleUp, long now, ScalingActivity prevScalingActivity, AutoScaleRule rule) {
        return scaleUp ?
                now > (Math.max(activeVmGroups.getLastSetAt(), prevScalingActivity.scaleUpAt) + rule.getCoolDownSecs() * 1000) :
                now > (Math.max(activeVmGroups.getLastSetAt(), Math.max(prevScalingActivity.scaleDownAt, prevScalingActivity.scaleUpAt))
                        + rule.getCoolDownSecs() * 1000);
    }

    private boolean shouldScaleUp(long now, ScalingActivity prevScalingActivity, AutoScaleRule rule) {
        return shouldScaleNow(true, now, prevScalingActivity, rule);
    }

    private boolean shouldScaleDown(long now, ScalingActivity prevScalingActivity, AutoScaleRule rule) {
        return shouldScaleNow(false, now, prevScalingActivity, rule);
    }

    private boolean shouldScaleDownInactive(long now, ScalingActivity prevScalingActivity, AutoScaleRule rule) {
        return now > (Math.max(activeVmGroups.getLastSetAt(), prevScalingActivity.inactiveScaleDownAt) + rule.getCoolDownSecs() * 1000);
    }

    private List<Runnable> processScalingNeeds(HostAttributeGroup hostAttributeGroup, ConcurrentMap<String, ScalingActivity> scalingActivityMap, AssignableVMs assignableVMs) {
        List<Runnable> callbacks = new ArrayList<>();
        AutoScaleRule rule = hostAttributeGroup.rule;
        long now = System.currentTimeMillis();
        ScalingActivity prevScalingActivity = scalingActivityMap.get(rule.getRuleName());

        int excess = hostAttributeGroup.shortFall > 0 ? 0 : hostAttributeGroup.idleHosts.size() - rule.getMaxIdleHostsToKeep();
        int inactiveIdleCount = hostAttributeGroup.idleInactiveHosts.size();

        List<String> allHostsToTerminate = new ArrayList<>();
        if (inactiveIdleCount > 0 && shouldScaleDownInactive(now, prevScalingActivity, rule)) {
            ScalingActivity scalingActivity = scalingActivityMap.get(rule.getRuleName());
            long lastReqstAge = (now - scalingActivity.inactiveScaleDownRequestedAt) / 1000L;
            if (delayScaleDownBySecs > 0L && lastReqstAge > 2 * delayScaleDownBySecs) { // reset the request at time
                scalingActivity.inactiveScaleDownRequestedAt = now;
            } else if (delayScaleDownBySecs == 0L || lastReqstAge > delayScaleDownBySecs) {
                scalingActivity.inactiveScaleDownRequestedAt = 0L;
                scalingActivity.inactiveScaleDownAt = now;
                Map<String, String> hostsToTerminate = getInactiveHostsToTerminate(hostAttributeGroup.idleInactiveHosts);
                if (logger.isDebugEnabled()) {
                    logger.debug("{} has an excess of {} inactive hosts ({})", rule.getRuleName(), hostsToTerminate.size(),
                            String.join(", ", hostsToTerminate.keySet()));
                }
                allHostsToTerminate.addAll(hostsToTerminate.values());
            }
        }

        if (excess > 0 && shouldScaleDown(now, prevScalingActivity, rule)) {
            ScalingActivity scalingActivity = scalingActivityMap.get(rule.getRuleName());
            long lastReqstAge = (now - scalingActivity.scaleDownRequestedAt) / 1000L;
            if (delayScaleDownBySecs > 0L && lastReqstAge > 2 * delayScaleDownBySecs) { // reset the request at time
                scalingActivity.scaleDownRequestedAt = now;
            } else if (delayScaleDownBySecs == 0L || lastReqstAge > delayScaleDownBySecs) {
                final int size = vmCollection.size(rule.getRuleName());
                if (rule.getMinSize() > (size - excess))
                    excess = Math.max(0, size - rule.getMinSize());
                if (excess > 0) {
                    scalingActivity.scaleDownRequestedAt = 0L;
                    scalingActivity.scaleDownAt = now;
                    scalingActivity.shortfall = hostAttributeGroup.shortFall;
                    Map<String, String> hostsToTerminate = getHostsToTerminate(hostAttributeGroup.idleHosts, excess);
                    scalingActivity.scaledNumInstances = hostsToTerminate.size();
                    scalingActivity.type = AutoScaleAction.Type.Down;
                    for (String host : hostsToTerminate.keySet()) {
                        long disabledDurationInSecs = Math.max(disabledVmDurationInSecs, rule.getCoolDownSecs());
                        assignableVMs.disableUntil(host, now + disabledDurationInSecs * 1000);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} has an excess of {} hosts ({})", rule.getRuleName(), hostsToTerminate.size(),
                                String.join(", ", hostsToTerminate.keySet()));
                    }
                    allHostsToTerminate.addAll(hostsToTerminate.values());
                }
            }
        } else if (hostAttributeGroup.shortFall > 0 || (excess <= 0 && shouldScaleUp(now, prevScalingActivity, rule))) {
            if (hostAttributeGroup.shortFall > 0 || rule.getMinIdleHostsToKeep() > hostAttributeGroup.idleHosts.size()) {
                // scale up to rule.getMaxIdleHostsToKeep() instead of just until rule.getMinIdleHostsToKeep()
                // but, if not shouldScaleUp(), then, scale up due to shortfall
                ScalingActivity scalingActivity = scalingActivityMap.get(rule.getRuleName());
                long lastReqstAge = (now - scalingActivity.scaleUpRequestedAt) / 1000L;
                if (delayScaleUpBySecs > 0L && lastReqstAge > 2 * delayScaleUpBySecs) { // reset the request at time
                    scalingActivity.scaleUpRequestedAt = now;
                } else if (delayScaleUpBySecs == 0L || lastReqstAge > delayScaleUpBySecs) {
                    int shortage = (excess <= 0 && shouldScaleUp(now, prevScalingActivity, rule)) ?
                            rule.getMaxIdleHostsToKeep() - hostAttributeGroup.idleHosts.size() : 0;
                    shortage = Math.max(shortage, hostAttributeGroup.shortFall);
                    final int size = vmCollection.size(rule.getRuleName());
                    if (shortage + size > rule.getMaxSize())
                        shortage = Math.max(0, rule.getMaxSize() - size);
                    if (shortage > 0) {
                        scalingActivity.scaleUpRequestedAt = 0L;
                        scalingActivity.scaleUpAt = now;
                        scalingActivity.shortfall = hostAttributeGroup.shortFall;
                        scalingActivity.scaledNumInstances = shortage;
                        scalingActivity.type = AutoScaleAction.Type.Up;
                        int finalShortage = shortage;
                        logger.debug("{} has a shortage of {} hosts", rule.getRuleName(), finalShortage);
                        callbacks.add(() -> {
                            logger.debug("Executing callback to scale up {} by {} hosts", rule.getRuleName(), finalShortage);
                            callback.call(new ScaleUpAction(rule.getRuleName(), finalShortage));
                        });
                    }
                }
            }
        }
        if (!allHostsToTerminate.isEmpty()) {
            callbacks.add(() -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Executing callback to scale down {} by {} hosts ({})", rule.getRuleName(), allHostsToTerminate.size(),
                            String.join(", ", allHostsToTerminate));
                }
                callback.call(new ScaleDownAction(rule.getRuleName(), allHostsToTerminate));
            });
        }

        return callbacks;
    }

    private void populateIdleResources(List<VirtualMachineLease> idleResources,
                                       List<VirtualMachineLease> idleInactiveResources,
                                       Map<String, HostAttributeGroup> leasesMap) {
        // Leases from active clusters
        for (VirtualMachineLease l : idleResources) {
            getAttribute(l).ifPresent(attrValue -> {
                if (leasesMap.containsKey(attrValue)) {
                    if (!leasesMap.get(attrValue).rule.idleMachineTooSmall(l))
                        leasesMap.get(attrValue).idleHosts.add(l);
                }
            });
        }

        // Leases from inactive clusters
        for (VirtualMachineLease l : idleInactiveResources) {
            getAttribute(l).ifPresent(attrValue -> {
                if (leasesMap.containsKey(attrValue)) {
                    leasesMap.get(attrValue).idleInactiveHosts.add(l);
                }
            });
        }
    }

    private Optional<String> getAttribute(VirtualMachineLease lease) {
        boolean hasValue = lease.getAttributeMap() != null
                && lease.getAttributeMap().get(attributeName) != null
                && lease.getAttributeMap().get(attributeName).getText().hasValue();
        return hasValue ? Optional.of(lease.getAttributeMap().get(attributeName).getText().getValue()) : Optional.empty();
    }

    private Map<String, HostAttributeGroup> setupHostAttributeGroupMap(AutoScaleRules autoScaleRules, ConcurrentMap<String, ScalingActivity> lastScalingAt) {
        Map<String, HostAttributeGroup> leasesMap = new HashMap<>();
        for (AutoScaleRule rule : autoScaleRules.getRules()) {
            leasesMap.put(rule.getRuleName(),
                    new HostAttributeGroup(rule.getRuleName(), rule));
            long initialCoolDown = getInitialCoolDown(rule.getCoolDownSecs());
            lastScalingAt.putIfAbsent(rule.getRuleName(), new ScalingActivity(initialCoolDown, initialCoolDown, 0, 0, null));
        }
        return leasesMap;
    }

    // make scaling activity happen after a fixed delayed time for the first time encountered (e.g., server start)
    private long getInitialCoolDown(long coolDownSecs) {
        long initialCoolDownInPastSecs = 120;
        initialCoolDownInPastSecs = Math.min(coolDownSecs, initialCoolDownInPastSecs);
        return System.currentTimeMillis() - coolDownSecs * 1000 + initialCoolDownInPastSecs * 1000;
    }

    private Map<String, String> getHostsToTerminate(List<VirtualMachineLease> idleHosts, int excess) {
        if (scaleDownConstraintExecutor == null) {
            return idleHosts.isEmpty() ? Collections.emptyMap() : getHostsToTerminateLegacy(idleHosts, excess);
        } else {
            return getHostsToTerminateUsingCriteria(idleHosts, excess);
        }
    }

    private Map<String, String> getInactiveHostsToTerminate(List<VirtualMachineLease> inactiveIdleHosts) {
        if (scaleDownConstraintExecutor == null) {
            return Collections.emptyMap();
        } else {
            Map<String, String> hostsMap = new HashMap<>();
            List<VirtualMachineLease> result = scaleDownConstraintExecutor.evaluate(inactiveIdleHosts);
            result.forEach(lease -> hostsMap.put(lease.hostname(), getMappedHostname(lease)));
            return hostsMap;
        }
    }

    private Map<String, String> getHostsToTerminateUsingCriteria(List<VirtualMachineLease> idleHosts, int excess) {
        Map<String, String> hostsMap = new HashMap<>();

        List<VirtualMachineLease> allIdle = new ArrayList<>(idleHosts);
        List<VirtualMachineLease> result = scaleDownConstraintExecutor.evaluate(allIdle);

        // The final result should contain only excess number of active hosts. Enforce this invariant.
        Set<String> activeIds = idleHosts.stream().map(VirtualMachineLease::hostname).collect(Collectors.toSet());
        int activeCounter = 0;
        Iterator<VirtualMachineLease> it = result.iterator();
        while (it.hasNext()) {
            VirtualMachineLease leaseToRemove = it.next();
            if (activeIds.contains(leaseToRemove.hostname())) {
                if (activeCounter < excess) {
                    activeCounter++;
                } else {
                    it.remove();
                }
            }
        }
        result.forEach(lease -> hostsMap.put(lease.hostname(), getMappedHostname(lease)));

        return hostsMap;
    }

    private Map<String, String> getHostsToTerminateLegacy(List<VirtualMachineLease> hosts, int excess) {
        Map<String, String> result = new HashMap<>();
        final Map<String, List<VirtualMachineLease>> hostsMap = new HashMap<>();
        final String defaultAttributeName = "default";
        for (VirtualMachineLease host : hosts) {
            final Protos.Attribute attribute = host.getAttributeMap().get(scaleDownBalancedByAttributeName);
            String val = (attribute != null && attribute.hasText()) ? attribute.getText().getValue() : defaultAttributeName;
            if (hostsMap.get(val) == null)
                hostsMap.put(val, new ArrayList<VirtualMachineLease>());
            hostsMap.get(val).add(host);
        }
        final List<List<VirtualMachineLease>> lists = new ArrayList<>();
        for (List<VirtualMachineLease> l : hostsMap.values())
            lists.add(l);
        int taken = 0;
        while (taken < excess) {
            List<VirtualMachineLease> takeFrom = null;
            int max = 0;
            for (List<VirtualMachineLease> l : lists) {
                if (l.size() > max) {
                    max = l.size();
                    takeFrom = l;
                }
            }
            final VirtualMachineLease removed = takeFrom.remove(0);
            result.put(removed.hostname(), getMappedHostname(removed));
            taken++;
        }
        return result;
    }

    private String getMappedHostname(VirtualMachineLease lease) {
        if (mapHostnameAttributeName == null || mapHostnameAttributeName.isEmpty())
            return lease.hostname();
        Protos.Attribute attribute = lease.getAttributeMap().get(mapHostnameAttributeName);
        if (attribute == null) {
            logger.error("Didn't find attribute " + mapHostnameAttributeName + " for host " + lease.hostname());
            return lease.hostname();
        }
        return attribute.getText().getValue();
    }

    public void setCallback(Action1<AutoScaleAction> callback) {
        this.callback = callback;
    }

    void shutdown() {
        if (isShutdown.compareAndSet(false, true))
            executor.shutdown();
    }

    private static class HostAttributeGroup {
        String name;
        List<VirtualMachineLease> idleHosts;
        List<VirtualMachineLease> idleInactiveHosts;
        int shortFall;
        AutoScaleRule rule;

        private HostAttributeGroup(String name, AutoScaleRule rule) {
            this.name = name;
            this.rule = rule;
            this.idleHosts = new ArrayList<>();
            this.idleInactiveHosts = new ArrayList<>();
            this.shortFall = 0;
        }
    }

    private static class ScalingActivity {
        private long scaleUpAt;
        private long scaleUpRequestedAt;
        private long scaleDownAt;
        private long scaleDownRequestedAt;
        private long inactiveScaleDownAt;
        private long inactiveScaleDownRequestedAt;
        private int shortfall;
        private int scaledNumInstances;
        private AutoScaleAction.Type type;

        private ScalingActivity(long scaleUpAt, long scaleDownAt, int shortfall, int scaledNumInstances, AutoScaleAction.Type type) {
            this.scaleUpAt = scaleUpAt;
            scaleUpRequestedAt = 0L;
            this.scaleDownAt = scaleDownAt;
            scaleDownRequestedAt = 0L;
            this.shortfall = shortfall;
            this.scaledNumInstances = scaledNumInstances;
            this.type = type;
        }
    }
}