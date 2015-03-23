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
