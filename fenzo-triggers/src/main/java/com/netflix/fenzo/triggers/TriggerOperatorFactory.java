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

package com.netflix.fenzo.triggers;

import com.netflix.fenzo.triggers.persistence.InMemoryTriggerDao;
import com.netflix.fenzo.triggers.persistence.TriggerDao;

/**
 * Simple factory to get a {@code TriggerOperator} instance
 *
 */
public class TriggerOperatorFactory {

    /**
     * Returns a default instance of {@code TriggerOperator} with sensible default values.
     * Uses an in-memory implementation of Dao.
     * @return
     */
    public static TriggerOperator getInstance() {
        return new TriggerOperator(new InMemoryTriggerDao(), 20);
    }

    /**
     *
     * @param threadPoolSize
     * @return
     */
    public static TriggerOperator getInstance(int threadPoolSize) {
        return new TriggerOperator(new InMemoryTriggerDao(), threadPoolSize);
    }

    /**
     *
     * @param triggerDao
     * @param threadPoolSize
     * @return
     */
    public static TriggerOperator getInstance(TriggerDao triggerDao, int threadPoolSize) {
        return new TriggerOperator(triggerDao, threadPoolSize);
    }
}
