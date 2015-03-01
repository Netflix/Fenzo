package io.mantisrx.fenzo;

import java.util.Collection;

public class ScaleDownAction implements AutoScaleAction {
    private final String ruleName;
    private final Collection<String> hosts;

    ScaleDownAction(String ruleName, Collection<String> hosts) {
        this.ruleName = ruleName;
        this.hosts = hosts;
        // ToDo need to ensure those hosts' offers don't get used
    }

    @Override
    public String getRuleName() {
        return ruleName;
    }

    @Override
    public Type getType() {
        return Type.Down;
    }

    public Collection<String> getHosts() {
        return hosts;
    }
}
