package com.netflix.fenzo.functions;

public interface Action2<T1, T2> {
    void call(T1 t1, T2 t2);
}
