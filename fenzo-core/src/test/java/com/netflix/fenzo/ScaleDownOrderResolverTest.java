package com.netflix.fenzo;

import com.netflix.fenzo.ScaleDownConstraintEvaluator.Result;
import com.netflix.fenzo.ScaleDownConstraintEvaluator.Selector;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ScaleDownOrderResolverTest {

    @Test
    public void testInOrderSplit() throws Exception {
        ScaleDownOrderResolver result = new ScaleDownOrderResolver(asList(
                resultOf(Selector.InOrder, asList("A", "B", "C", "D", "X"), asList("E", "F", "G", "H", "Y"), singletonList("X")),
                resultOf(Selector.InOrder, asList("B", "D", "E", "F", "Y"), asList("A", "C", "G", "H", "X"), singletonList("Y"))
        ));

        assertThat(removableIdsOf(result), is(equalTo(asList("B", "D", "A", "C", "E", "F", "G", "H"))));
        assertThat(notRemovableIdsOf(result), is(equalTo(asList("X", "Y"))));
    }

    @Test
    public void testSingleBalancedSplit() throws Exception {
        ScaleDownOrderResolver result = new ScaleDownOrderResolver(asList(
                resultOf(Selector.Balanced, asList("1a", "2a", "3a"), asList("1b", "2b", "3b"), singletonList("4a"))
        ));

        List<Character> ids = removableIdsOf(result).stream().map(z -> z.charAt(z.length() - 1)).collect(Collectors.toList());
        for (int i = 0; i < ids.size(); i += 2) {
            assertThat(ids.get(i) == 'a' || ids.get(i + 1) == 'a', is(true));
            assertThat(ids.get(i) == 'b' || ids.get(i + 1) == 'b', is(true));
        }
        assertThat(notRemovableIdsOf(result), is(equalTo(singletonList("4a"))));
    }

    @Test
    public void testInOrderBalancedSplit() throws Exception {
        ScaleDownOrderResolver result = new ScaleDownOrderResolver(asList(
                resultOf(Selector.InOrder, asList("1a", "1b"), asList("2a", "2b", "3a", "3b"), singletonList("4a")),
                resultOf(Selector.Balanced, asList("1a", "2a", "3a"), asList("1b", "2b", "3b"), singletonList("4a"))
        ));

        List<String> ids = removableIdsOf(result);
        assertThat(ids.get(0).equals("1a") || ids.get(0).equals("1b"), is(true));
        assertThat(ids.get(1).equals("1a") || ids.get(1).equals("1b"), is(true));

        assertThat(notRemovableIdsOf(result), is(equalTo(singletonList("4a"))));
    }

    private Result resultOf(Selector selector, List<String> sliceOne, List<String> sliceTwo, List<String> notRemovable) {
        Map<VirtualMachineLease, String> notRemovableMap = new HashMap<>();
        notRemovable.forEach(name -> notRemovableMap.put(lease(name), "Locked"));
        return new Result(selector,
                asList(
                        sliceOne.stream().map(ScaleDownOrderResolverTest::lease).collect(Collectors.toSet()),
                        sliceTwo.stream().map(ScaleDownOrderResolverTest::lease).collect(Collectors.toSet())
                ),
                notRemovableMap
        );
    }

    private List<String> notRemovableIdsOf(ScaleDownOrderResolver result) {
        return result.getNotRemovable().keySet().stream().map(VirtualMachineLease::getId).collect(Collectors.toList());
    }

    private List<String> removableIdsOf(ScaleDownOrderResolver result) {
        return result.getVmsInScaleDownOrder().stream().map(VirtualMachineLease::getId).collect(Collectors.toList());
    }

    private static VirtualMachineLease lease(String name) {
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return name;
            }

            @Override
            public long getOfferedTime() {
                return 0;
            }

            @Override
            public String hostname() {
                return null;
            }

            @Override
            public String getVMID() {
                return null;
            }

            @Override
            public double cpuCores() {
                return 0;
            }

            @Override
            public double memoryMB() {
                return 0;
            }

            @Override
            public double networkMbps() {
                return 0;
            }

            @Override
            public double diskMB() {
                return 0;
            }

            @Override
            public List<Range> portRanges() {
                return null;
            }

            @Override
            public Protos.Offer getOffer() {
                return null;
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return null;
            }

            @Override
            public Double getScalarValue(String name) {
                return null;
            }

            @Override
            public Map<String, Double> getScalarValues() {
                return null;
            }
        };
    }
}