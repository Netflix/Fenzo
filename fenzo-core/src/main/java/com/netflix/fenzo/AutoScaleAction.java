package com.netflix.fenzo;

public interface AutoScaleAction {
    public enum Type {Up, Down};

    public String getRuleName();
    public Type getType();
}
