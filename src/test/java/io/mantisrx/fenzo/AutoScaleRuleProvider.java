package io.mantisrx.fenzo;

import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.VirtualMachineLease;

public class AutoScaleRuleProvider {

    static AutoScaleRule createRule(final String name, final int min, final int max, final long coolDownSecs,
                                    final double cpuTooSmall, final double memoryTooSmall) {
        return new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return name;
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return min;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return max;
            }

            @Override
            public long getCoolDownSecs() {
                return coolDownSecs;
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return (lease.cpuCores()<cpuTooSmall || lease.memoryMB()<memoryTooSmall);
            }
        };
    }
}
