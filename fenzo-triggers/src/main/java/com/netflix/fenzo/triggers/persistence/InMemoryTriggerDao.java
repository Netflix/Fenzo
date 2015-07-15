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
    public void deleteTrigger(String triggerGroup, Trigger trigger) {
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
