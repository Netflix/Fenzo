package com.netflix.fenzo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AutoScaleRules {
    private final Map<String, AutoScaleRule> ruleMap;

    AutoScaleRules() {
        ruleMap = new HashMap<>();
    }

    public void addRule(AutoScaleRule rule) {
        ruleMap.put(rule.getRuleName(), rule);
    }

    public AutoScaleRule get(String attrValue) {
        return ruleMap.get(attrValue);
    }

    Collection<AutoScaleRule> getRules() {
        return ruleMap.values();
    }
}
