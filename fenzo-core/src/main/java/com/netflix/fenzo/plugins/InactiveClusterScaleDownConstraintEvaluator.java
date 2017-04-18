package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * VM constraint evaluator that splits all VMs into two equivalence classes: inactive and active, with
 * inactive class being first to scale down.
 */
public class InactiveClusterScaleDownConstraintEvaluator implements ScaleDownConstraintEvaluator {

    private final Function<VirtualMachineLease, Boolean> isInactive;

    public InactiveClusterScaleDownConstraintEvaluator(Function<VirtualMachineLease, Boolean> isInactive) {
        this.isInactive = isInactive;
    }

    @Override
    public Result evaluate(Collection<VirtualMachineLease> candidates) {
        Set<VirtualMachineLease> active = new HashSet<>();
        Set<VirtualMachineLease> inactive = new HashSet<>();
        candidates.forEach(l -> {
            if (isInactive(l)) {
                inactive.add(l);
            } else {
                active.add(l);
            }
        });
        inactive.addAll(active);

        return new Result(Selector.InOrder, asList(inactive, active), Collections.emptyMap());
    }

    private boolean isInactive(VirtualMachineLease lease) {
        try {
            Boolean inactive = isInactive.apply(lease);
            return inactive != null && inactive;
        } catch (Exception ignore) {
            // We expect callback provider to handle all exceptions, and this is just a safeguard for Fenzo evaluator
        }
        return false;
    }
}
