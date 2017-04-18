package com.netflix.fenzo;

import com.netflix.fenzo.ScaleDownConstraintEvaluator.Result;
import com.netflix.fenzo.ScaleDownConstraintEvaluator.Selector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class is doing a post-processing of {@link ScaleDownConstraintEvaluator} results. Each result contains
 * an ordered list of leases collected into equivalence groups. The merging process starts with the first group
 * in the list, and dividing it further according to the division criteria provided by the subsequent groups.
 * <p>
 * Lets take as an example eight leases A, B, C, D, E, F, G, H:
 * <ul>
 *     <li>First equivalence order is based on active/inactive criteria: inactive=A,B,C,D and active=E,F,G,H</li>
 *     <li>Second is based on software age: old=B,D,E,F and new=A,C,G,H</li>
 * </ul>
 * The merged split is: (inactive,old)=B,D and (inactive,new)=A,C and (active,old)=E,F and (active,new)=G,H. This means
 * that the first VMs to terminate are B or D (order does not matter here), next are A or C, etc.
 * In a similar way we may split first servers by active/inactive state, and next within each group split them further
 * according to zone balancing criteria.
 *
 * TODO Invariants checking that all results have full set of VMs?
 * TODO There are multiple optimizations that can be done for equivalence group merging. This is just first prototype.
 */
class ScaleDownOrderResolver {

    private static final Comparator<VirtualMachineLease> ID_COMPARATOR = Comparator.comparing(VirtualMachineLease::getId);

    private final List<VirtualMachineLease> vmsInScaleDownOrder;
    private final Map<VirtualMachineLease, String> notRemovable;

    ScaleDownOrderResolver(List<Result> results) {
        if (results.isEmpty()) {
            this.vmsInScaleDownOrder = Collections.emptyList();
            this.notRemovable = Collections.emptyMap();
        } else if (results.size() == 1) {
            Result first = results.get(0);
            this.vmsInScaleDownOrder = flatten(partition(first.getSelector(), first.getEquivalenceGroups()));
            this.notRemovable = first.getNotRemovable();
        } else {
            this.notRemovable = findAllNotRemovable(results);
            this.vmsInScaleDownOrder = doOrder(results, notRemovable.keySet());
        }
    }

    List<VirtualMachineLease> getVmsInScaleDownOrder() {
        return vmsInScaleDownOrder;
    }

    Map<VirtualMachineLease, String> getNotRemovable() {
        return notRemovable;
    }

    String toCompactString() {
        List<String> removableHosts = vmsInScaleDownOrder.stream().map(VirtualMachineLease::hostname).collect(Collectors.toList());
        List<String> notRemovableHosts = notRemovable.keySet().stream().map(VirtualMachineLease::hostname).collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();
        sb.append("{scaleDownOrder=").append(removableHosts);
        sb.append(", notRemovable=").append(notRemovableHosts);
        sb.append('}');
        return sb.toString();
    }

    private Map<VirtualMachineLease, String> findAllNotRemovable(List<Result> results) {
        Map<VirtualMachineLease, String> notRemovable = new TreeMap<>(ID_COMPARATOR);
        results.forEach(r -> notRemovable.putAll(r.getNotRemovable()));
        return notRemovable;
    }

    private List<VirtualMachineLease> doOrder(List<Result> results,
                                              Set<VirtualMachineLease> notRemovable) {
        Result first = results.get(0);
        List<Set<VirtualMachineLease>> scaleDownOrder = partition(first.getSelector(), first.getEquivalenceGroups());

        for (int i = 1; i < results.size(); i++) {
            Result next = results.get(i);
            if (next.getSelector() == Selector.InOrder) {
                scaleDownOrder = splitInOrder(scaleDownOrder, next.getEquivalenceGroups());
            } else { // Balanced
                scaleDownOrder = splitBalanced(scaleDownOrder, next.getEquivalenceGroups());
            }
        }
        List<VirtualMachineLease> all = flatten(scaleDownOrder);
        all.removeAll(notRemovable);
        return all;
    }

    private List<Set<VirtualMachineLease>> partition(Selector selector, List<Set<VirtualMachineLease>> equivalenceGroups) {
        return selector == Selector.InOrder ? equivalenceGroups : partitionBalanced(equivalenceGroups);
    }

    private List<Set<VirtualMachineLease>> partitionBalanced(List<Set<VirtualMachineLease>> equivalenceGroups) {
        List<Set<VirtualMachineLease>> remaining = equivalenceGroups.stream().map(HashSet::new).collect(Collectors.toList());
        List<Set<VirtualMachineLease>> balanced = new ArrayList<>();
        while (!remaining.isEmpty()) {
            Set<VirtualMachineLease> chunk = new HashSet<>();
            Iterator<Set<VirtualMachineLease>> it = remaining.iterator();
            while (it.hasNext()) {
                Set<VirtualMachineLease> next = it.next();
                if (!next.isEmpty()) {
                    Iterator<VirtualMachineLease> sit = next.iterator();
                    chunk.add(sit.next());
                    sit.remove();
                }
                if (next.isEmpty()) {
                    it.remove();
                }
            }
            balanced.add(chunk);
        }

        return balanced;
    }

    private List<Set<VirtualMachineLease>> splitInOrder(List<Set<VirtualMachineLease>> scaleDownOrder, List<Set<VirtualMachineLease>> divider) {
        return scaleDownOrder.stream().flatMap(g -> splitInOrder(g, divider).stream()).collect(Collectors.toList());
    }

    private List<Set<VirtualMachineLease>> splitInOrder(Set<VirtualMachineLease> group, List<Set<VirtualMachineLease>> divider) {
        Set<VirtualMachineLease> remaining = new TreeSet<>(ID_COMPARATOR);
        remaining.addAll(group);

        List<Set<VirtualMachineLease>> result = new ArrayList<>();
        for (int i = 0; i < divider.size() && !remaining.isEmpty(); i++) {
            Set<VirtualMachineLease> slice = new TreeSet<>(ID_COMPARATOR);
            slice.addAll(divider.get(i));
            slice.retainAll(remaining);
            result.add(slice);
            remaining.removeAll(slice);
        }
        return result;
    }

    private List<Set<VirtualMachineLease>> splitBalanced(List<Set<VirtualMachineLease>> scaleDownOrder, List<Set<VirtualMachineLease>> divider) {
        return scaleDownOrder.stream().flatMap(g -> splitBalanced(g, divider).stream()).collect(Collectors.toList());
    }

    private List<Set<VirtualMachineLease>> splitBalanced(Set<VirtualMachineLease> group, List<Set<VirtualMachineLease>> divider) {
        Set<VirtualMachineLease> orderedGroup = new TreeSet<>(ID_COMPARATOR);
        orderedGroup.addAll(group);

        List<Set<VirtualMachineLease>> trimmed = new ArrayList<>();
        divider.forEach(d -> {
            Set<VirtualMachineLease> trimmedGroup = new TreeSet<>(ID_COMPARATOR);
            trimmedGroup.addAll(d);
            trimmedGroup.retainAll(orderedGroup);
            if (!trimmedGroup.isEmpty()) {
                trimmed.add(trimmedGroup);
            }
        });
        return partitionBalanced(trimmed);
    }

    private List<VirtualMachineLease> flatten(List<Set<VirtualMachineLease>> scaleDownOrder) {
        return scaleDownOrder.stream().flatMap(Set::stream).collect(Collectors.toList());
    }
}
