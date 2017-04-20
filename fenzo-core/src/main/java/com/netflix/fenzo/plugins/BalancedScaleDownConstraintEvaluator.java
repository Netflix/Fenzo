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

import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.VirtualMachineLease;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Evaluator that tries to keep the same number of idle instances in each zone/region.
 * As the number of instances may be large, and for each subsequent instance within a group we need to increase the
 * score, we add exponentially increasing increments to the initial scoring.
 *
 * <h1>Example</h1>
 * Lets assume that:
 * <ul>
 *     <li>There are two zones Za and Zb</li>
 *     <li>Zone Za has five idle instances, and zone Zb three</li>
 *     <li>The initial score is 0.5, and the initial step is 0.1</li>
 * </ul>
 * The subsequent scores for a single zone will be: 0.5, 0.6, 0.65, 0.675, 0.6875, etc.
 * Int the given example we will get:
 * <ul>
 *     <li>Za(1)=0.5, Za(2)=0.6, Za(3)=0.65, Za(4)=0.675, Za(5)=0.6875</li>
 *     <li>Zb(1)=0.5, Zb(2)=0.6, Zb(3)=0.65</li>
 * </ul>
 * Assuming the zone balancing is the only evaluation criteria, the termination order (according to the highest score)
 * for four instances will be: Za(5), Za(4), Za(3), Zb(3). In the end zones Za and Zb will be left with the same
 * number of instances.
 */
public class BalancedScaleDownConstraintEvaluator implements ScaleDownConstraintEvaluator<Map<String, Integer>> {

    // TODO Figure our failure propagation without blowing up everything
    private static final String FAILURE_GROUP = "failures";

    private final Function<VirtualMachineLease, String> keyExtractor;
    private final double initialScore;
    private final double initialStep;

    public BalancedScaleDownConstraintEvaluator(Function<VirtualMachineLease, String> keyExtractor,
                                                double initialScore,
                                                double initialStep) {
        this.keyExtractor = keyExtractor;
        this.initialScore = initialScore;
        this.initialStep = initialStep;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public Result<Map<String, Integer>> evaluate(VirtualMachineLease candidate, Optional<Map<String, Integer>> optionalContext) {
        Map<String, Integer> context = optionalContext.orElseGet(HashMap::new);
        String group = findGroup(candidate);

        int groupCount = context.getOrDefault(group, 0);
        context.put(group, groupCount + 1);

        double score = computeScore(groupCount);

        return Result.of(score, context);
    }

    private double computeScore(int groupCount) {
        if (groupCount == 0) {
            return initialScore;
        }
        return initialScore + initialStep * (1 - Math.pow(0.5, groupCount)) / 0.5;
    }

    private String findGroup(VirtualMachineLease l) {
        String groupName;
        try {
            groupName = keyExtractor.apply(l);
        } catch (Exception e) {
            groupName = FAILURE_GROUP;
        }
        return groupName;
    }
}
