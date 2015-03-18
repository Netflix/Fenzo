package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Trigger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class InMemoryTriggerDao implements TriggerDao {

    private final ConcurrentMap<String, Map<String,Trigger>> groupedTriggers = new ConcurrentHashMap<String, Map<String,Trigger>>();
    private final ConcurrentMap<String, Trigger> triggers = new ConcurrentHashMap<String, Trigger>();

    @Override
    public void createTrigger(String triggerGroup, Trigger trigger) {
        Map triggerMap = new HashMap();
        triggerMap.put(trigger.getId(), trigger);
        Map existingTriggerMap = groupedTriggers.putIfAbsent(triggerGroup, triggerMap);
        if (existingTriggerMap != null) {
            synchronized (groupedTriggers) {
                groupedTriggers.get(triggerGroup).put(trigger.getId(), trigger);
            }
        }
        triggers.put(trigger.getId(), trigger);
    }

    @Override
    public Trigger getTrigger(String triggerId) {
        return triggers.get(triggerId);
    }

    @Override
    public void updateTrigger(String triggerGroup, Trigger trigger) {
        createTrigger(triggerGroup, trigger);
    }

    @Override
    public void deleteTrigger(String triggerGroup, Trigger trigger) {
        groupedTriggers.get(triggerGroup).remove(trigger.getId());
        triggers.remove(trigger.getId());
    }

    @Override
    public List<Trigger> getTriggers(String triggerGroup) {
        return (List<Trigger>) groupedTriggers.get(triggerGroup).values();
    }

    @Override
    public List<Trigger> getTriggers() {
        return (List<Trigger>) triggers.values();
    }
}
