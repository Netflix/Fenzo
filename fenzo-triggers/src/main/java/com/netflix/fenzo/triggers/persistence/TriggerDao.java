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

/**
 *
 */
public interface TriggerDao {

    public void createTrigger(String triggerGroup, Trigger trigger);
    public Trigger getTrigger(String triggerGroup, String triggerId);
    public void updateTrigger(String triggerGroup, Trigger trigger);
    public void deleteTrigger(String triggerGroup, Trigger trigger);
    public List<Trigger> getTriggers(String triggerGroup);
    public List<Trigger> getTriggers();
}
