/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ScaleDownOrderEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * VM constraint evaluator that splits all VMs into two equivalence classes: inactive and active, with
 * inactive class being first to scale down.
 */
public class InactiveClusterScaleDownConstraintEvaluator implements ScaleDownOrderEvaluator {

    private final Function<VirtualMachineLease, Boolean> isInactive;

    public InactiveClusterScaleDownConstraintEvaluator(Function<VirtualMachineLease, Boolean> isInactive) {
        this.isInactive = isInactive;
    }

    @Override
    public List<Set<VirtualMachineLease>> evaluate(Collection<VirtualMachineLease> candidates) {
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

        return asList(inactive, active);
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
