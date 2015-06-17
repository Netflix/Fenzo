package com.netflix.fenzo.functions;

/**
 * A one-argument action.
 * @param <T>
 */
public interface Action1<T> {
    public void call(T t);
}
