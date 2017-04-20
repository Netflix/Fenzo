package com.netflix.fenzo;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.netflix.fenzo.plugins.TestableVirtualMachineLease.leasesWithIds;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScaleDownConstraintExecutorTest {

    private final ScaleDownOrderEvaluator orderEvaluator = mock(ScaleDownOrderEvaluator.class);
    private final ScaleDownConstraintEvaluator<Void> constraintEvaluator = mock(ScaleDownConstraintEvaluator.class);

    private final ScaleDownConstraintExecutor executor = new ScaleDownConstraintExecutor(
            orderEvaluator, singletonList(constraintEvaluator)
    );

    @Test
    public void testScaleDownOrdering() throws Exception {
        List<VirtualMachineLease> candidates = leasesWithIds("l0", "l1", "l2", "l3", "l4");

        when(orderEvaluator.evaluate(candidates)).thenReturn(
                asList(asSet(candidates.get(0), candidates.get(2), candidates.get(3)), asSet(candidates.get(1), candidates.get(4)))
        );
        when(constraintEvaluator.evaluate(any(), any())).then(a -> {
            String id = ((VirtualMachineLease) a.getArgument(0)).getId();
            int idx = Integer.parseInt("" + id.charAt(1));
            return ScaleDownConstraintEvaluator.Result.of(idx == 0 ? 0 : idx / 10.0);
        });

        List<VirtualMachineLease> order = executor.evaluate(candidates);
        List<String> ids = order.stream().map(VirtualMachineLease::getId).collect(Collectors.toList());
        assertThat(ids, is(equalTo(asList("l3", "l2", "l4", "l1"))));
    }

    private static <T> Set<T> asSet(T... values) {
        Set<T> result = new HashSet<>();
        Collections.addAll(result, values);
        return result;
    }
}