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
package com.netflix.fenzo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link ScaleDownConstraintExecutor} uses {@link ScaleDownOrderEvaluator} and multiple
 * {@link ScaleDownConstraintEvaluator}s to determine termination order of candidate VM list. The evaluation process
 * includes the following steps:
 * <ul>
 *     <li>{@link ScaleDownOrderEvaluator} splits candidate VMs into ordered list of equivalence groups</li>
 *     <li>For each equivalence group multiple {@link ScaleDownOrderEvaluator}s are executed to produce a score for each VM</li>
 *     <li>VMs are ordered in each equivalence group with the highest score first</li>
 *     <li>The ordered equivalence groups are merged into single list, preserving the partitioning order computed by
 *         {@link ScaleDownConstraintExecutor}. This list is returned as a result.
 *     </li>
 * </ul>
 */
class ScaleDownConstraintExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ScaleDownConstraintExecutor.class);

    private static final double NOT_REMOVABLE_MARKER = -1;

    private final ScaleDownOrderEvaluator orderEvaluator;
    private final Map<ScaleDownConstraintEvaluator, Double> scoringEvaluators;

    ScaleDownConstraintExecutor(ScaleDownOrderEvaluator orderEvaluator, Map<ScaleDownConstraintEvaluator, Double> weightedScoringEvaluators) {
        checkArguments(weightedScoringEvaluators);
        this.orderEvaluator = orderEvaluator;
        this.scoringEvaluators = weightedScoringEvaluators;
    }

    List<VirtualMachineLease> evaluate(Collection<VirtualMachineLease> candidates) {
        List<Set<VirtualMachineLease>> fixedOrder = orderEvaluator.evaluate(candidates);

        List<VirtualMachineLease> scaleDownOrder = scoringEvaluators.isEmpty()
                ? fixedOrder.stream().flatMap(Set::stream).collect(Collectors.toList())
                : fixedOrder.stream().flatMap(this::groupEvaluator).collect(Collectors.toList());

        if (logger.isDebugEnabled()) {
            List<String> hosts = scaleDownOrder.stream().map(VirtualMachineLease::hostname).collect(Collectors.toList());
            logger.debug("Evaluated scale down order: {}", hosts);
        }

        return scaleDownOrder;
    }

    private void checkArguments(Map<ScaleDownConstraintEvaluator, Double> weightedScoringEvaluators) {
        List<String> evaluatorsWithInvalidWeights = weightedScoringEvaluators.entrySet().stream()
                .filter(e -> e.getValue() <= 0)
                .map(e -> e.getKey().getName() + '=' + e.getValue())
                .collect(Collectors.toList());
        if (!evaluatorsWithInvalidWeights.isEmpty()) {
            throw new IllegalArgumentException("Evaluator weighs must be > 0. This invariant is violated by " + evaluatorsWithInvalidWeights);
        }
    }

    private Stream<VirtualMachineLease> groupEvaluator(Set<VirtualMachineLease> groupCandidates) {
        Map<VirtualMachineLease, Double> scores = new HashMap<>();

        scoringEvaluators.forEach((e, weight) -> {
            Optional<? super Object> optionalContext = Optional.empty();
            for (VirtualMachineLease lease : groupCandidates) {
                double currentScore = scores.getOrDefault(lease, 0.0);
                if (currentScore != NOT_REMOVABLE_MARKER) {
                    ScaleDownConstraintEvaluator.Result result = e.evaluate(lease, optionalContext);
                    double newScore = result.getScore() * weight;
                    if (newScore == 0) {
                        scores.put(lease, NOT_REMOVABLE_MARKER);
                    } else {
                        scores.put(lease, currentScore + newScore);
                    }
                    optionalContext = result.getContext();
                }
            }
        });

        return scores.entrySet().stream()
                .filter(e -> e.getValue() != NOT_REMOVABLE_MARKER)
                .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue())) // Descending order
                .map(Map.Entry::getKey);
    }
}
