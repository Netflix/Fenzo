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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import rx.functions.Action1;

import java.util.Date;

/**
 * Base class for all types of triggers
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
public class Trigger<T> {

    private String id;
    private final Date createdDate;
    private T data;
    private final Class<T> dataType;
    private final Class<? extends Action1<? extends T>> action;
    private final String name;
    private boolean disabled;

    @JsonCreator
    public Trigger(@JsonProperty("name") String name,
                   @JsonProperty("data") T data,
                   @JsonProperty("dataType") Class<T> dataType,
                   @JsonProperty("action") Class<? extends Action1<T>> action) {
        this.createdDate = new Date();
        this.name = name;
        this.data = data;
        this.dataType = dataType;
        this.action = action;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * @warn method description missing
     * @warn parameter id description missing
     *
     * @param id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public Date getCreatedDate() {
        return createdDate;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public T getData() {
        return data;
    }

    /**
     * @warn method description missing
     * @warn parameter data description missing
     *
     * @param data
     */
    public void setData(T data) {
        this.data = data;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public Class<T> getDataType() {
        return dataType;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public Class<? extends Action1<? extends T>> getAction() {
        return action;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public boolean isDisabled() {
        return disabled;
    }

    /**
     * @warn method description missing
     * @warn parameter disabled description missing
     *
     * @param disabled
     */
    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    /**
     * @warn method description missing
     *
     * @return
     */
    public String toString() {
        return "Trigger (" + id + ":" + name + ")";
    }

}
