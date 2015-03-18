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
