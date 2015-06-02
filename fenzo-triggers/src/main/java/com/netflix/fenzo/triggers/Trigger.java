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

import rx.functions.Action1;

import java.util.Date;
import java.util.UUID;

/**
 * Base class for all types of triggers
 */
public class Trigger<T> {

    private final String id;
    private final Date createdDate;
    private final T data;
    private final Action1<? extends T> action;
    private final String name;
    private boolean disabled;

    public Trigger(String name, T data, Action1<T> action) {
        this.id = UUID.randomUUID().toString();
        this.createdDate = new Date();
        this.name = name;
        this.data = data;
        this.action = action;
    }

    public String getId() {
        return id;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public T getData() {
        return data;
    }

    public Action1<? extends T> getAction() {
        return action;
    }

    public String getName() {
        return name;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public String toString() {
        return "Trigger (" + id + ":" + name + ")";
    }

}
