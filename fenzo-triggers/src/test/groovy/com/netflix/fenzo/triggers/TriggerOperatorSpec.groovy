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
import com.netflix.fenzo.triggers.CronTrigger
import com.netflix.fenzo.triggers.Trigger
import com.netflix.fenzo.triggers.TriggerOperator
import rx.functions.Action1
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author sthadeshwar
 */
class TriggerOperatorSpec extends Specification {

    @Shared TriggerDao triggerDao = new InMemoryTriggerDao()
    @Shared TriggerOperator triggerOperator = new TriggerOperator(triggerDao, 1)

    class Data {
        String status
        Data(String status) { this.status = status }
    }

    static class TestAction implements Action1<Data> {
        @Override
        void call(Data foo) { foo.status = 'executed' }
    }

    void 'test register trigger'() {
        when:
        Trigger<Data> registerTrigger = new Trigger<Data>('trigger1', new Data('registerTrigger'), Data.class, TestAction.class)
        String triggerId = triggerOperator.registerTrigger('api', registerTrigger)

        then:
        triggerOperator.getTrigger(triggerId) != null
        triggerDao.getTrigger(triggerId) != null
    }

    void 'test execute trigger'() {
        when:
        Trigger<Data> executeTrigger = new Trigger<Data>('trigger2', new Data('executeTrigger'), Data.class, TestAction.class)
        String triggerId = triggerOperator.registerTrigger('api', executeTrigger)
        triggerOperator.execute(triggerId)

        then:
        noExceptionThrown()
        executeTrigger.data != null
        executeTrigger.data.status == 'executed'
    }

    void 'test disable/enable trigger'() {
        when:
        Trigger<Data> disableEnableTrigger = new Trigger<Data>('trigger2', new Data('disableEnableTrigger'), Data.class, TestAction.class)
        String triggerId = triggerOperator.registerTrigger('api', disableEnableTrigger)
        triggerOperator.disableTrigger(triggerId)
        Trigger<Data> disabledTrigger = triggerOperator.getTrigger(triggerId)

        then:
        noExceptionThrown()
        disabledTrigger != null
        disabledTrigger.id == disableEnableTrigger.id
        disabledTrigger.disabled == true

        when:
        triggerOperator.execute(disabledTrigger.id)

        then:
        disabledTrigger.data.status == 'disableEnableTrigger'

        when:
        triggerOperator.enableTrigger(disabledTrigger)
        Trigger<Data> enabledTrigger = triggerOperator.getTrigger(disabledTrigger.id)

        then:
        enabledTrigger != null
        enabledTrigger.id == disableEnableTrigger.id
        enabledTrigger.disabled == false

        when:
        triggerOperator.execute(enabledTrigger.id)

        then:
        enabledTrigger.data.status == 'executed'
    }

    void 'test schedule trigger'() {
        setup:
        triggerOperator.initialize()

        when:
        Trigger<Data> scheduledTrigger =
            new CronTrigger<Data>('0/5 * * * * ?', 'trigger2', new Data('scheduledTrigger'), Data.class, TestAction.class)
        String triggerId = triggerOperator.registerTrigger('api', scheduledTrigger)
        Thread.sleep(10*1000L)
        Trigger<Data> executedTrigger = triggerOperator.getTrigger(triggerId)

        then:
        executedTrigger.data.status == 'executed'

        cleanup:
        triggerOperator.destroy()
    }

}
