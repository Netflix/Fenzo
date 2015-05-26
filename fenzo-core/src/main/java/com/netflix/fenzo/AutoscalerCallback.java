package com.netflix.fenzo;

public interface AutoscalerCallback {
    void process(AutoScaleAction action);
}
