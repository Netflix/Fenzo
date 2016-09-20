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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class maintains the set of autoscaling rules. You may define different autoscaling rules for different
 * sets of hosts. For example, you may create an autoscaling group for a certain sort of workload and populate it
 * with machines with a certain set of capabilities specific to that workload, and a second autoscaling group for
 * a more varied set of tasks that requires a more varied or flexible set of machines.
 */
class AutoScaleRules {
    private final Map<String, AutoScaleRule> ruleMap;
    private final BlockingQueue<AutoScaleRule> addQ = new LinkedBlockingQueue<>();
    private final List<AutoScaleRule> addList = new LinkedList<>();
    private final BlockingQueue<String> remQ = new LinkedBlockingQueue<>();
    private final List<String> remList = new LinkedList<>();

    AutoScaleRules(List<AutoScaleRule> autoScaleRules) {
        ruleMap = new HashMap<>();
        if(autoScaleRules!=null && !autoScaleRules.isEmpty())
            for(AutoScaleRule r: autoScaleRules)
                ruleMap.put(r.getRuleName(), r);
    }

    /**
     * Adds or modifies an autoscale rule in the set of active rules. Pass in the {@link AutoScaleRule} you want
     * to add or modify. If another {@code AutoScaleRule} with the same rule name already exists, it will be
     * overwritten by {@code rule}. Otherwise, {@code rule} will be added to the active set of autoscale rules.
     *
     * @param rule the autoscale rule you want to add or modify
     */
    void replaceRule(AutoScaleRule rule) {
        if(rule != null)
            addQ.offer(rule);
    }

    /**
     * Removes an autoscale rule from the set of active rules. Pass in the name of the autoscale rule you want to
     * unqueueTask (this is the same name that would be returned by its
     * {@link AutoScaleRule#getRuleName getRuleName()} method).
     *
     * @param ruleName the rule name of the autoscale rule you want to unqueueTask from the set of active rules
     */
    void remRule(String ruleName) {
        if(ruleName != null)
            remQ.offer(ruleName);
    }

    void prepare() {
        addQ.drainTo(addList);
        if(!addList.isEmpty()) {
            final Iterator<AutoScaleRule> iterator = addList.iterator();
            while(iterator.hasNext()) {
                final AutoScaleRule r = iterator.next();
                if(r != null && r.getRuleName()!=null)
                    ruleMap.put(r.getRuleName(), r);
                iterator.remove();
            }
        }
        remQ.drainTo(remList);
        if(!remList.isEmpty()) {
            final Iterator<String> iterator = remList.iterator();
            while(iterator.hasNext()) {
                final String name = iterator.next();
                if(name != null)
                    ruleMap.remove(name);
                iterator.remove();
            }
        }
    }

    /**
     * Returns the autoscale rule whose name is equal to a specified String. The name of an autoscale rule is the
     * value of the attribute that defines the type of host the rule applies to (for instance, the name of the
     * autoscaling group).
     *
     * @param attrValue the name of the autoscale rule you want to retrieve
     * @return the autoscale rule whose name is equal to {@code attrValue}
     */
    public AutoScaleRule get(String attrValue) {
        return ruleMap.get(attrValue);
    }

    Collection<AutoScaleRule> getRules() {
        return ruleMap.values();
    }
}
