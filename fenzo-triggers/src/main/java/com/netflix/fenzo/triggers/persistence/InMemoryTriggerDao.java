package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Trigger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of {@code TriggerDao}
 */
public class InMemoryTriggerDao implements TriggerDao {

    private final ConcurrentMap<String, ConcurrentMap<String,Trigger>> groupedTriggers = new ConcurrentHashMap<String, ConcurrentMap<String,Trigger>>();

    @Override
    public void createTrigger(String triggerGroup, Trigger trigger) {
        ConcurrentMap<String, Trigger> triggerMap = new ConcurrentHashMap<String, Trigger>();
        triggerMap.put(trigger.getId(), trigger);
        Map existingTriggerMap = groupedTriggers.putIfAbsent(triggerGroup, triggerMap);
        if (existingTriggerMap != null) {
            synchronized (groupedTriggers) {
                groupedTriggers.get(triggerGroup).put(trigger.getId(), trigger);
            }
        }
    }

    @Override
    public Trigger getTrigger(String triggerGroup, String triggerId) {
        ConcurrentMap<String, Trigger> triggersMap = groupedTriggers.get(triggerGroup);
        for (Iterator<String> iterator2 = triggersMap.keySet().iterator(); iterator2.hasNext();) {
            String storedTriggerId = iterator2.next();
            if (triggerId.equals(storedTriggerId)) {
                return triggersMap.get(triggerId);
            }
        }
        return null;
    }

    @Override
    public void updateTrigger(String triggerGroup, Trigger trigger) {
        createTrigger(triggerGroup, trigger);
    }

    @Override
    public void deleteTrigger(String triggerGroup, Trigger trigger) {
        groupedTriggers.get(triggerGroup).remove(trigger.getId());
    }

    @Override
    public List<Trigger> getTriggers(String triggerGroup) {
        return new ArrayList<Trigger>(groupedTriggers.get(triggerGroup).values());
    }

    @Override
    public List<Trigger> getTriggers() {
        List<Trigger> triggerList = new ArrayList<Trigger>();
        for (Iterator<String> iterator1 = groupedTriggers.keySet().iterator(); iterator1.hasNext();) {
            ConcurrentMap<String, Trigger> triggersMap = groupedTriggers.get(iterator1.next());
            triggerList.addAll(triggersMap.values());
        }
        return triggerList;
    }
}
