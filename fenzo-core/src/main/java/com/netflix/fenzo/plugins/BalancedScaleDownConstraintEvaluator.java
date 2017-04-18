package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.*;
import java.util.function.Function;

/**
 * Evaluator that tries to keep the same number of idle instances in each zone/region.
 */
public class BalancedScaleDownConstraintEvaluator implements ScaleDownConstraintEvaluator {

    // TODO Figure our failure propagation without blowing up everything
    private static final String FAILURE_GROUP = "failures";

    private final Function<VirtualMachineLease, String> keyExtractor;

    public BalancedScaleDownConstraintEvaluator(Function<VirtualMachineLease, String> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public Result evaluate(Collection<VirtualMachineLease> candidates) {
        return new Result(Selector.Balanced, groupBy(candidates), Collections.emptyMap());
    }

    private List<Set<VirtualMachineLease>> groupBy(Collection<VirtualMachineLease> candidates) {
        Map<String, Set<VirtualMachineLease>> groups = new HashMap<>();
        candidates.forEach(l -> {
            String groupName;
            try {
                groupName = keyExtractor.apply(l);
            } catch (Exception e) {
                groupName = FAILURE_GROUP;
            }
            groups.computeIfAbsent(groupName, g -> new HashSet<>()).add(l);
        });
        return new ArrayList<>(groups.values());
    }
}
