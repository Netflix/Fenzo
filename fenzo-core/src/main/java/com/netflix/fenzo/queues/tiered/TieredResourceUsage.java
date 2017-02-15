package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.ResourceUsage;
import com.netflix.fenzo.queues.ResourceUsage.CompositeResourceUsage;
import com.netflix.fenzo.queues.ResourceUsage.LeafResourceUsage;
import com.netflix.fenzo.sla.ResAllocs;

import java.util.Optional;

/**
 * Tiered resources form a hierarchy, with {@link Tier} being top-level entity, aggregating multiple {@link QueueBucket}s.
 * {@link QueueBucket} in turn, aggregates {@link QueuableTask}s.
 */
class TieredResourceUsage {

    static class GuaranteedResAllocs {
        private final ResAllocs actual;
        private final ResAllocs effective;

        GuaranteedResAllocs(ResAllocs actual) {
            this(actual, actual);
        }

        GuaranteedResAllocs(ResAllocs actual, ResAllocs effective) {
            this.actual = actual;
            this.effective = effective;
        }

        ResAllocs getActual() {
            return actual;
        }

        ResAllocs getEffective() {
            return effective;
        }
    }

    static LeafResourceUsage<GuaranteedResAllocs, QueuableTask> newBucket(String name, ResourceUsage<GuaranteedResAllocs> parent, TierSla tierSla) {
        return new LeafResourceUsage<>(
                parent,
                new GuaranteedResAllocs(ResAllocsUtil.emptyOf(name)),
                (total, newTask) -> addTaskToBucket(total, newTask, tierSla),
                (total, toRemove) -> removeTaskFromBucket(total, toRemove, tierSla)
        );
    }

    static CompositeResourceUsage<GuaranteedResAllocs> newTier(String tierName) {
        return new CompositeResourceUsage<>(
                Optional.empty(),
                TieredResourceUsage::addBucketToTier,
                TieredResourceUsage::removeBucketFromTier,
                new GuaranteedResAllocs(ResAllocsUtil.emptyOf(tierName))
        );
    }

    private static GuaranteedResAllocs addTaskToBucket(GuaranteedResAllocs total, QueuableTask newTask, TierSla tierSla) {
        return newGuaranteedResAllocsOf(ResAllocsUtil.add(total.getActual(), newTask), tierSla);
    }

    private static GuaranteedResAllocs removeTaskFromBucket(GuaranteedResAllocs total, QueuableTask toRemove, TierSla tierSla) {
        return newGuaranteedResAllocsOf(ResAllocsUtil.subtract(total.getActual(), toRemove), tierSla);
    }

    private static GuaranteedResAllocs newGuaranteedResAllocsOf(ResAllocs newActual, TierSla tierSla) {
        return tierSla.getResAllocs(newActual.getTaskGroupName())
                .map(bucketAllocs -> new GuaranteedResAllocs(newActual, ResAllocsUtil.ceilingOf(newActual, bucketAllocs)))
                .orElseGet(() -> new GuaranteedResAllocs(newActual));
    }

    private static GuaranteedResAllocs addBucketToTier(GuaranteedResAllocs total, GuaranteedResAllocs bucketToAdd) {
        return new GuaranteedResAllocs(
                ResAllocsUtil.add(total.getActual(), bucketToAdd.getActual()),
                ResAllocsUtil.add(total.getEffective(), bucketToAdd.getEffective())
        );
    }

    private static GuaranteedResAllocs removeBucketFromTier(GuaranteedResAllocs total, GuaranteedResAllocs bucketToRemove) {
        return new GuaranteedResAllocs(
                ResAllocsUtil.subtract(total.getActual(), bucketToRemove.getActual()),
                ResAllocsUtil.subtract(total.getEffective(), bucketToRemove.getEffective())
        );
    }
}
