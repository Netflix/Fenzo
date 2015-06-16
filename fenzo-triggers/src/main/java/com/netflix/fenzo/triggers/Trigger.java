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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Class<T> getDataType() {
        return dataType;
    }

    public Class<? extends Action1<? extends T>> getAction() {
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
