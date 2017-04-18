package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.*;
import java.util.function.Function;

/**
 * VM constraint evaluator that prevents VMs matching a configurable criteria from being terminated.
 */
public class LockedVmScaleDownConstraintEvaluator implements ScaleDownConstraintEvaluator {

    private final Function<VirtualMachineLease, Boolean> isLocked;

    public LockedVmScaleDownConstraintEvaluator(Function<VirtualMachineLease, Boolean> isLocked) {
        this.isLocked = isLocked;
    }

    @Override
    public Result evaluate(Collection<VirtualMachineLease> candidates) {
        Set<VirtualMachineLease> notLocked = new HashSet<>();
        Map<VirtualMachineLease, String> locked = new HashMap<>();

        candidates.forEach(l -> {
            if (isLocked(l)) {
                locked.put(l, "Not allowed to be terminated");
            } else {
                notLocked.add(l);
            }
        });

        return new Result(Selector.InOrder, Collections.singletonList(notLocked), locked);
    }

    private boolean isLocked(VirtualMachineLease lease) {
        try {
            Boolean locked = isLocked.apply(lease);
            return locked != null && locked;
        } catch (Exception ignore) {
            // We expect callback provider to handle all exceptions, and this is just a safeguard for Fenzo evaluator
        }
        return false;
    }
}
