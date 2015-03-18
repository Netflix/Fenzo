package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Trigger;

import java.util.List;

/**
 *
 */
public interface TriggerDao {

    public void createTrigger(String triggerGroup, Trigger trigger);
    public Trigger getTrigger(String triggerId);
    public void updateTrigger(String triggerGroup, Trigger trigger);
    public void deleteTrigger(String triggerGroup, Trigger trigger);
    public List<Trigger> getTriggers(String triggerGroup);
    public List<Trigger> getTriggers();
}
