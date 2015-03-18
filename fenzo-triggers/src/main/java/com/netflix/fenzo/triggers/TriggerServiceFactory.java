package com.netflix.fenzo.triggers;

import com.netflix.fenzo.triggers.persistence.InMemoryTriggerDao;
import com.netflix.fenzo.triggers.persistence.TriggerDao;

/**
 * Simple factory to get a {@code TriggerService} instance
 *
 */
public class TriggerServiceFactory {

    /**
     * Returns a default instance of {@code TriggerService} with sensible default values.
     * Uses an in-memory implementation of Dao.
     * @return
     */
    public static TriggerService getInstance() {
        return new TriggerService(new InMemoryTriggerDao(), 20);
    }

    /**
     *
     * @param threadPoolSize
     * @return
     */
    public static TriggerService getInstance(int threadPoolSize) {
        return new TriggerService(new InMemoryTriggerDao(), threadPoolSize);
    }

    /**
     *
     * @param triggerDao
     * @param threadPoolSize
     * @return
     */
    public static TriggerService getInstance(TriggerDao triggerDao, int threadPoolSize) {
        return new TriggerService(triggerDao, threadPoolSize);
    }
}
