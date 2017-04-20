package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ScaleDownOrderEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.*;

public class NoOpScaleDownOrderEvaluator implements ScaleDownOrderEvaluator {
    @Override
    public List<Set<VirtualMachineLease>> evaluate(Collection<VirtualMachineLease> candidates) {
        Set<VirtualMachineLease> singleGroup = new HashSet<>();
        singleGroup.addAll(candidates);
        return Collections.singletonList(singleGroup);
    }
}
