package com.netflix.fenzo;

import java.util.concurrent.atomic.AtomicBoolean;

class StateMonitor {
    private final AtomicBoolean lock;

    StateMonitor() {
        lock = new AtomicBoolean(false);
    }

    AutoCloseable enter() {
        if(!lock.compareAndSet(false, true))
            throw new IllegalStateException();
        return new AutoCloseable() {
            @Override
            public void close() throws Exception {
                if(!lock.compareAndSet(true, false))
                    throw new IllegalStateException();
            }
        };
    }

}
