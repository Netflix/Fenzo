package com.netflix.fenzo;

/**
 * A rule to define the behavior for auto scaling the number of hosts of a certain type. Rules are defined
 * per unique value of host attribute that is used for matching.
 */
public interface AutoScaleRule {
    /**
     * Value of the host attribute to match to apply this rule.
     * @return Value of matching host attribute.
     */
    public String getRuleName();

    public int getMinIdleHostsToKeep();

    public int getMaxIdleHostsToKeep();

    public long getCoolDownSecs();

    /**
     * Predicate to check if an idle host has too few resources to be considered idle. This is used to filter out hosts
     * with too few resources before considering them as excess resources. If not filtered out, they could prevent a
     * much needed scale up action.
     * @param lease
     * @return
     */
    public boolean idleMachineTooSmall(VirtualMachineLease lease);
}
