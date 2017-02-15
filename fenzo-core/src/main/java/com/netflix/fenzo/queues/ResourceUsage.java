package com.netflix.fenzo.queues;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * A hierarchical representation of resources. For tiered queues, this will be tier / queue / task hierarchy.
 * Updates at any level, trigger cascading updates up the tree.
 */
public abstract class ResourceUsage<U> {

    public abstract U getUsage();

    public abstract boolean refresh();

    public static class LeafResourceUsage<USAGE, ALLOC> extends ResourceUsage<USAGE> {

        private final ResourceUsage<USAGE> parent;
        private final BiFunction<USAGE, ALLOC, USAGE> addFun;
        private final BiFunction<USAGE, ALLOC, USAGE> subtractFun;

        private USAGE usage;

        public LeafResourceUsage(ResourceUsage<USAGE> parent,
                                 USAGE initial,
                                 BiFunction<USAGE, ALLOC, USAGE> addFun,
                                 BiFunction<USAGE, ALLOC, USAGE> subtractFun) {
            this.parent = parent;
            this.addFun = addFun;
            this.subtractFun = subtractFun;
            this.usage = initial;
        }

        @Override
        public USAGE getUsage() {
            return usage;
        }

        public void add(ALLOC allocation) {
            usage = addFun.apply(usage, allocation);
            parent.refresh();
        }

        public void remove(ALLOC allocation) {
            usage = subtractFun.apply(usage, allocation);
            parent.refresh();
        }

        @Override
        public boolean refresh() {
            return parent.refresh();
        }
    }

    public static class CompositeResourceUsage<USAGE> extends ResourceUsage<USAGE> {

        private final ResourceUsage<USAGE> parent;
        private final BiFunction<USAGE, USAGE, USAGE> addFun;
        private final BiFunction<USAGE, USAGE, USAGE> subtractFun;

        private final Map<String, ResourceUsage<USAGE>> children = new HashMap<>();
        private final Map<String, USAGE> childrenRecordedUsage = new HashMap<>();
        private USAGE totalUsage;

        public CompositeResourceUsage(Optional<ResourceUsage<USAGE>> parent,
                                      BiFunction<USAGE, USAGE, USAGE> addFun,
                                      BiFunction<USAGE, USAGE, USAGE> subtractFun,
                                      USAGE zero) {
            this.parent = parent.orElse(null);
            this.addFun = addFun;
            this.subtractFun = subtractFun;
            this.totalUsage = zero;
        }

        @Override
        public USAGE getUsage() {
            return totalUsage;
        }

        public void add(String name, ResourceUsage<USAGE> subResource) {
            ResourceUsage<USAGE> previous = children.get(name);
            if (previous != null) {
                subtractFun.apply(totalUsage, childrenRecordedUsage.get(name));
            }

            USAGE usage = subResource.getUsage();
            addFun.apply(totalUsage, usage);

            children.put(name, subResource);
            childrenRecordedUsage.put(name, usage);
        }

        public void remove(String name) {
            ResourceUsage<USAGE> previous = children.remove(name);
            if (previous != null) {
                subtractFun.apply(totalUsage, childrenRecordedUsage.remove(name));
            }
        }

        @Override
        public boolean refresh() {
            return parent != null && parent.refresh();
        }

        public void refresh(String childName) {
            ResourceUsage<USAGE> subResource = children.get(childName);
            if (subResource != null) {
                add(childName, subResource);
            }
        }
    }
}
