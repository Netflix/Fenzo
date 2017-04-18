package com.netflix.fenzo;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A constraint evaluator that examines if VMs can be terminated, and in which order. Some example evaluation criteria:
 * <ul>
 *     <li>Is VM allowed to be terminated</li>
 *     <li>Does VM belong to deactivated server group, that should be terminated first</li>
 *     <li>Does VM run older version of the software compared to other candidates</li>
 * </ul>
 *
 * The evaluator groups VMs into equivalence classes, and if the selector type is 'InOrder', orders them according
 * to their scale down priority. If selector type is 'Balanced' no ordering is done, as each equivalent group
 * should be used in (weighted) round robin fashion.
 * Documentation in {@link ScaleDownOrderResolver} explains how results from multiple evaluators are combined together.
 *
 * The evaluation result {@link ScaleDownConstraintEvaluator.Result} should contain all VMs exactly ones. If the
 * contract is violated, the final result is undefined.
 *
 * TODO It would be useful to label equivalence groups for diagnostic purposes.
 */
public interface ScaleDownConstraintEvaluator {

    enum Selector {InOrder, Balanced}

    /**
     * The result of the evaluation of a {@link ScaleDownConstraintEvaluator}.
     */
    class Result {

        /**
         * Selector mechanism to apply for the given result.
         */
        private final Selector selector;

        /**
         * Each value in the list represents VMs with the same termination priority (belonging to the same
         * equivalence class).
         */
        private final List<Set<VirtualMachineLease>> scaleDownOrder;

        /**
         * Map of instances that should not be terminated, with values providing a human readable explanation.
         */
        private final Map<VirtualMachineLease, String> notRemovable;

        public Result(Selector selector, List<Set<VirtualMachineLease>> scaleDownOrder, Map<VirtualMachineLease, String> notRemovable) {
            this.selector = selector;
            this.scaleDownOrder = scaleDownOrder;
            this.notRemovable = notRemovable;
        }

        public Selector getSelector() {
            return selector;
        }

        public List<Set<VirtualMachineLease>> getEquivalenceGroups() {
            return scaleDownOrder;
        }

        public Map<VirtualMachineLease, String> getNotRemovable() {
            return notRemovable;
        }
    }

    /**
     * Returns the name of the constraint evaluator.
     *
     * @return the name of the constraint evaluator
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Inspects a target to decide whether or not it meets the constraints appropriate to a particular task.
     *
     * @param candidates all candidates to be terminated. Fenzo may choose to terminate only some of them.
     * @return a successful Result if the target meets the constraints enforced by this constraint evaluator, or
     *         an unsuccessful Result otherwise
     */
    Result evaluate(Collection<VirtualMachineLease> candidates);
}
