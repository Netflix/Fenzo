package com.netflix.fenzo.triggers;

import com.netflix.fenzo.triggers.persistence.EventDao;
import com.netflix.fenzo.triggers.persistence.InMemoryEventDao;
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
        return new TriggerService(new InMemoryTriggerDao(), new InMemoryEventDao(), 20);
    }

    /**
     *
     * @param threadPoolSize
     * @return
     */
    public static TriggerService getInstance(int threadPoolSize) {
        return new TriggerService(new InMemoryTriggerDao(), new InMemoryEventDao(), threadPoolSize);
    }

    /**
     *
     * @param triggerDao
     * @param eventDao
     * @param threadPoolSize
     * @return
     */
    public static TriggerService getInstance(TriggerDao triggerDao, EventDao eventDao, int threadPoolSize) {
        return new TriggerService(triggerDao, eventDao, threadPoolSize);
    }
}
