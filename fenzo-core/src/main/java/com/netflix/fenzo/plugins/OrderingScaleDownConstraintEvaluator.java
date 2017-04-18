package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.*;
import java.util.function.Function;

/**
 * Splits VMs into equivalence groups using a classifier, and next orders them with the provided {@link Comparator}.
 */
public class OrderingScaleDownConstraintEvaluator<GROUP_ID> implements ScaleDownConstraintEvaluator {

    private final Function<VirtualMachineLease, GROUP_ID> classifier;
    private final Comparator<GROUP_ID> equivalentGroupComparator;

    public OrderingScaleDownConstraintEvaluator(Function<VirtualMachineLease, GROUP_ID> classifier,
                                                Comparator<GROUP_ID> equivalentGroupComparator) {
        this.classifier = classifier;
        this.equivalentGroupComparator = equivalentGroupComparator;
    }

    @Override
    public Result evaluate(Collection<VirtualMachineLease> candidates) {
        Map<GROUP_ID, Set<VirtualMachineLease>> groupedLeases = new HashMap<>();
        Map<VirtualMachineLease, String> failures = new HashMap<>();
        candidates.forEach(l -> {
            try {
                groupedLeases.computeIfAbsent(classifier.apply(l), g -> new HashSet<>()).add(l);
            } catch (Exception e) {
                failures.put(l, "Equivalent group computation error: " + e.getMessage());
            }
        });

        List<GROUP_ID> orderedGroups = new ArrayList<>(groupedLeases.keySet());
        orderedGroups.sort(equivalentGroupComparator);

        List<Set<VirtualMachineLease>> scaleDownOrder = new ArrayList<>();
        orderedGroups.forEach(g -> scaleDownOrder.add(groupedLeases.get(g)));

        return new Result(Selector.InOrder, scaleDownOrder, failures);
    }
}
