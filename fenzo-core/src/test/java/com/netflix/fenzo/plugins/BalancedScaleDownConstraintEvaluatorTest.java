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

import com.netflix.fenzo.ScaleDownConstraintEvaluator.Result;
import com.netflix.fenzo.VirtualMachineLease;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static com.netflix.fenzo.plugins.TestableVirtualMachineLease.leaseWithId;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BalancedScaleDownConstraintEvaluatorTest {

    private BalancedScaleDownConstraintEvaluator evaluator;

    @Test
    public void testBalancing() throws Exception {
        evaluator = new BalancedScaleDownConstraintEvaluator(
                this::keyExtractor, 0.5, 0.1
        );

        Optional<Map<String, Integer>> context = evaluateAndVerify(leaseWithId("l1"), Optional.empty(), 0.5);
        evaluateAndVerify(leaseWithId("l2"), context, 0.6);
        evaluateAndVerify(leaseWithId("l3"), context, 0.65);
        evaluateAndVerify(leaseWithId("l4"), context, 0.675);
    }

    private String keyExtractor(VirtualMachineLease lease) {
        return "myZone";
    }

    private Optional<Map<String, Integer>> evaluateAndVerify(VirtualMachineLease lease,
                                                             Optional<Map<String, Integer>> context,
                                                             double expectedScore) {
        Result<Map<String, Integer>> result = evaluator.evaluate(lease, context);
        assertThat(result.getScore(), is(equalTo(expectedScore)));
        return result.getContext();
    }
}