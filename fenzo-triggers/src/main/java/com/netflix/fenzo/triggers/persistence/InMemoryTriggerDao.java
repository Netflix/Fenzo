package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Trigger;

import java.util.List;
import java.util.UUID;

/**
 * In-memory implementation of {@code TriggerDao}
 */
public class InMemoryTriggerDao extends AbstractInMemoryDao<Trigger> implements TriggerDao {

    @Override
    public String createTrigger(String triggerGroup, Trigger trigger) {
        trigger.setId(createId(triggerGroup, UUID.randomUUID().toString()));
        create(triggerGroup, trigger.getId(), trigger);
        return trigger.getId();
    }

    @Override
    public Trigger getTrigger(String triggerId) {
        String triggerGroup = extractGroupFromId(triggerId);
        return read(triggerGroup, triggerId);
    }

    @Override
    public void updateTrigger(Trigger trigger) {
        String triggerGroup = extractGroupFromId(trigger.getId());
        update(triggerGroup, trigger.getId(), trigger);
    }

    @Override
    public void deleteTrigger(Trigger trigger) {
        String triggerGroup = extractGroupFromId(trigger.getId());
        delete(triggerGroup, trigger.getId());
    }

    @Override
    public List<Trigger> getTriggers(String triggerGroup) {
        return list(triggerGroup);
    }

    @Override
    public List<Trigger> getTriggers() {
        return list();
    }

}
