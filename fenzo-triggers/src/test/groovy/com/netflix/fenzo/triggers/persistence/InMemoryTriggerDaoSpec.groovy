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

package com.netflix.fenzo.triggers.persistence

import com.netflix.fenzo.triggers.Trigger
import spock.lang.Shared
import spock.lang.Specification

/**
 *
 * @author sthadeshwar
 */
class InMemoryTriggerDaoSpec extends Specification {

    @Shared TriggerDao triggerDao = new InMemoryTriggerDao()
    class Foo {}

    void 'test create trigger'() {
        when:
        String group = 'api'
        String triggerId = triggerDao.createTrigger(group, new Trigger<Foo>('trigger1', null, Foo.class, null))
        Trigger savedTrigger = triggerDao.getTrigger(triggerId)

        then:
        savedTrigger.id == triggerId
        savedTrigger.name == 'trigger1'
    }

    void 'test update trigger'() {
        when:
        String group = 'api'
        String triggerId = triggerDao.createTrigger(group, new Trigger<Foo>('trigger1', null, Foo.class, null))
        Trigger savedTrigger = triggerDao.getTrigger(triggerId)

        then:
        savedTrigger.id == triggerId
        savedTrigger.name == 'trigger1'
        savedTrigger.disabled == false

        when:
        savedTrigger.disabled = true
        triggerDao.updateTrigger(savedTrigger)
        Trigger updatedTrigger = triggerDao.getTrigger(savedTrigger.id)

        then:
        updatedTrigger.id == savedTrigger.id
        updatedTrigger.name == 'trigger1'
        updatedTrigger.disabled == true
    }

    void 'test list triggers'() {
        when:
        String group = 'api'
        String triggerId1 = triggerDao.createTrigger(group, new Trigger<Foo>('trigger1', null, Foo.class, null))
        String triggerId2 = triggerDao.createTrigger(group, new Trigger<Foo>('trigger2', null, Foo.class, null))
        List<Trigger> triggers = triggerDao.getTriggers(group)

        then:
        triggers.find { it.id == triggerId1 } != null
        triggers.find { it.id == triggerId2 } != null
    }

    void 'test delete trigger'() {
        when:
        String group = 'api'
        String triggerId = triggerDao.createTrigger(group, new Trigger<Foo>('trigger1', null, Foo.class, null))
        Trigger savedTrigger = triggerDao.getTrigger(triggerId)

        then:
        savedTrigger.id == triggerId
        savedTrigger.name == 'trigger1'

        when:
        triggerDao.deleteTrigger(group, savedTrigger)
        Trigger deletedTrigger = triggerDao.getTrigger(triggerId)

        then:
        deletedTrigger == null
    }
}
