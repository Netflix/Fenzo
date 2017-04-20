package com.netflix.fenzo;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.*;
import java.util.stream.Collectors;

import static com.netflix.fenzo.plugins.TestableVirtualMachineLease.leasesWithIds;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScaleDownConstraintExecutorTest {

    private final ScaleDownOrderEvaluator orderEvaluator = mock(ScaleDownOrderEvaluator.class);

    private final ScaleDownConstraintEvaluator<Void> evenFirstEvaluator = mock(ScaleDownConstraintEvaluator.class);
    private final ScaleDownConstraintEvaluator<Void> oddFirstEvaluator = mock(ScaleDownConstraintEvaluator.class);

    private ScaleDownConstraintExecutor executor;

    @Before
    public void setUp() throws Exception {
        when(evenFirstEvaluator.getName()).thenReturn("evenFirstEvaluator");
        when(oddFirstEvaluator.getName()).thenReturn("oddFirstEvaluator");
    }

    @Test
    public void testScaleDownOrdering() throws Exception {
        Map<ScaleDownConstraintEvaluator, Double> scoringEvaluators = new HashMap<>();
        scoringEvaluators.put(evenFirstEvaluator, 10.0);
        scoringEvaluators.put(oddFirstEvaluator, 2.0);
        executor = new ScaleDownConstraintExecutor(orderEvaluator, scoringEvaluators);

        List<VirtualMachineLease> candidates = leasesWithIds("l0", "l1", "l2", "l3", "l4");

        when(orderEvaluator.evaluate(candidates)).thenReturn(
                asList(asSet(candidates.get(0), candidates.get(2), candidates.get(3)), asSet(candidates.get(1), candidates.get(4)))
        );
        when(evenFirstEvaluator.evaluate(any(), any())).then(a -> {
            int idx = leaseIndexOf(a);
            return ScaleDownConstraintEvaluator.Result.of(idx == 0 ? 0 : (idx % 2 == 0 ? 1 : 0.5));
        });
        when(oddFirstEvaluator.evaluate(any(), any())).then(a -> {
            int idx = leaseIndexOf(a);
            return ScaleDownConstraintEvaluator.Result.of(idx == 0 ? 0 : (idx % 2 == 1 ? 1 : 0.5));
        });

        List<VirtualMachineLease> order = executor.evaluate(candidates);
        List<String> ids = order.stream().map(VirtualMachineLease::getId).collect(Collectors.toList());
        assertThat(ids, is(equalTo(asList("l2", "l3", "l4", "l1"))));
    }

    @Test
    public void testWeightsValidation() throws Exception {
        Map<ScaleDownConstraintEvaluator, Double> scoringEvaluators = new HashMap<>();
        scoringEvaluators.put(evenFirstEvaluator, -5.0);
        scoringEvaluators.put(oddFirstEvaluator, 0.0);
        try {
            new ScaleDownConstraintExecutor(orderEvaluator, scoringEvaluators);
            fail("Expected to fail argument validation");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage().contains("evenFirstEvaluator=-5.0"), is(true));
            assertThat(e.getMessage().contains("oddFirstEvaluator=0.0"), is(true));
        }
    }

    private static <T> Set<T> asSet(T... values) {
        Set<T> result = new HashSet<>();
        Collections.addAll(result, values);
        return result;
    }

    private static int leaseIndexOf(InvocationOnMock invocation) {
        String id = ((VirtualMachineLease) invocation.getArgument(0)).getId();
        return Integer.parseInt("" + id.charAt(1));
    }
}