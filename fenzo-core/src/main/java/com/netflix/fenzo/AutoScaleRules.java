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
 * @warn class description missing
 */
public class AutoScaleRules {
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
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param rule
     */
    void replaceRule(AutoScaleRule rule) {
        if(rule != null)
            addQ.offer(rule);
    }

    /**
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param ruleName
     */
    void remRule(String ruleName) {
        if(ruleName != null)
            remQ.offer(ruleName);
    }

    /**
     * @warn method description missing
     */
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
     * @warn method description missing
     * @warn parameter description missing
     *
     * @param attrValue
     * @return
     */
    public AutoScaleRule get(String attrValue) {
        return ruleMap.get(attrValue);
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    Collection<AutoScaleRule> getRules() {
        return ruleMap.values();
    }
}
