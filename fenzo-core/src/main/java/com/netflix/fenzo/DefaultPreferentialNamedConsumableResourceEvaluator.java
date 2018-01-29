package com.netflix.fenzo;

/**
 * Default {@link PreferentialNamedConsumableResourceEvaluator} implementation.
 */
public class DefaultPreferentialNamedConsumableResourceEvaluator implements PreferentialNamedConsumableResourceEvaluator {

    public static final PreferentialNamedConsumableResourceEvaluator INSTANCE = new DefaultPreferentialNamedConsumableResourceEvaluator();

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        // unassigned: 0.0 indicates no fitness, so return 0.5, which is less than the case of assigned with 0 sub-resources
        return 0.5 / (subResourcesLimit + 1);
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        return Math.min(1.0, (subResourcesUsed + subResourcesNeeded + 1.0) / (subResourcesLimit + 1));
    }
}
