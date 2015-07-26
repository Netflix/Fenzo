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

package com.netflix.fenzo;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A monitor to ensure scheduler state is not compromised by concurrent calls that are disallowed.
 */
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
