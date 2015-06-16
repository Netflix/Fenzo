package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Trigger;

import java.util.List;

/**
 *
 */
public interface TriggerDao {

    public String createTrigger(String triggerGroup, Trigger trigger);
    public void updateTrigger(Trigger trigger);
    public Trigger getTrigger(String triggerId);
    public void deleteTrigger(Trigger trigger);
    public List<Trigger> getTriggers(String triggerGroup);
    public List<Trigger> getTriggers();
}
