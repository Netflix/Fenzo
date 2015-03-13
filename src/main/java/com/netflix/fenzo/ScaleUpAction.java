package com.netflix.fenzo;

public class ScaleUpAction implements AutoScaleAction {
    private final String ruleName;
    private final int scaleUpCount;

    ScaleUpAction(String ruleName, int scaleUpCount) {
        this.ruleName = ruleName;
        this.scaleUpCount = scaleUpCount;
    }

    @Override
    public Type getType() {
        return Type.Up;
    }

    @Override
    public String getRuleName() {
        return ruleName;
    }

    public int getScaleUpCount() {
        return scaleUpCount;
    }
}
