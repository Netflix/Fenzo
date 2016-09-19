/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.fenzo.queues;

import java.util.List;

/**
 * An exception that wraps multiple exceptions caught during a scheduling iteration of the queue, to be returned
 * at the end of scheduling iteration.
 */
public class TaskQueueMultiException extends Exception {

    private final List<Exception> exceptions;

    public TaskQueueMultiException(List<Exception> exceptions) {
        super("Multiple task queue exceptions");
        this.exceptions = exceptions;
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }
}
