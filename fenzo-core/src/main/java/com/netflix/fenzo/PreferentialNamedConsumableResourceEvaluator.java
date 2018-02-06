package com.netflix.fenzo;

import com.netflix.fenzo.PreferentialNamedConsumableResourceSet.PreferentialNamedConsumableResource;

/**
 * Evaluator for {@link PreferentialNamedConsumableResource} selection process. Given an agent with matching
 * ENI slot (either empty or with a matching name), this evaluator computes the fitness score.
 * A custom implementation can provide fitness calculators augmented with additional information not available to
 * Fenzo for making best placement decision.
 *
 * <h1>Example</h1>
 * {@link PreferentialNamedConsumableResource} can be used to model AWS ENI interfaces together with IP and security
 * group assignments. To minimize number of AWS API calls and to improve efficiency, it is beneficial to place a task
 * on an agent which has ENI profile with matching security group profile so the ENI can be reused. Or if a task
 * is terminated, but agent releases its resources lazily, they can be reused by another task with a matching profile.
 */
public interface PreferentialNamedConsumableResourceEvaluator {

    /**
     * Provide fitness score for an idle consumable resource.
     *
     * @param hostname hostname of an agent
     * @param resourceName name to be associated with a resource with the given index
     * @param index a consumable resource index
     * @param subResourcesNeeded an amount of sub-resources required by a scheduled task
     * @param subResourcesLimit a total amount of sub-resources available
     * @return fitness score
     */
    double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit);

    /**
     * Provide fitness score for a consumable resource that is already associated with some tasks. These tasks and
     * the current one having profiles so can share the resource.
     *
     * @param hostname hostname of an agent
     * @param resourceName name associated with a resource with the given index
     * @param index a consumable resource index
     * @param subResourcesNeeded an amount of sub-resources required by a scheduled task
     * @param subResourcesUsed an amount of sub-resources already used by other tasks
     * @param subResourcesLimit a total amount of sub-resources available
     * @return fitness score
     */
    double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit);
}
