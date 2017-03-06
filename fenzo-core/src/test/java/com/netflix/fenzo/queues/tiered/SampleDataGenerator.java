package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;
import com.netflix.fenzo.sla.ResAllocsUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test data generator.
 */
public class SampleDataGenerator {

    private int taskIdx;

    private final Map<Integer, TierState> tiers = new HashMap<>();

    public SampleDataGenerator addTier(int tierNumber, ResAllocs tierCapacity) {
        tiers.put(tierNumber, new TierState(tierNumber, tierCapacity));
        return this;
    }

    public SampleDataGenerator updateTier(int tier, ResAllocs tierCapacity) {
        TierState tierState = tiers.get(tier);
        if (tierState == null) {
            throw new IllegalArgumentException("Tier " + tier + " not defined");
        }
        tierState.updateTer(tierCapacity);
        return this;
    }

    public SampleDataGenerator addBucket(int tierNumber, String bucketName, ResAllocs bucketCapacity) {
        TierState tierState = tiers.computeIfAbsent(tierNumber, k -> new TierState(tierNumber, ResAllocsUtil.empty()));
        tierState.addBucket(bucketName, bucketCapacity);
        return this;
    }

    public void removeBucket(int tierNumber, String bucketName) {
        TierState tierState = tiers.get(tierNumber);
        if (tierState == null) {
            throw new IllegalArgumentException("Tier " + tierNumber + " not defined");
        }
        tierState.removeBucket(bucketName);
    }

    public static ResAllocs createResAllocs(int multiplier) {
        return createResAllocs("anonymous", multiplier);
    }

    public static ResAllocs createResAllocs(String name, int multiplier) {
        return new ResAllocsBuilder(name)
                .withCores(1.0 * multiplier)
                .withMemory(1024 * multiplier)
                .withNetworkMbps(128 * multiplier)
                .withDisk(32 * multiplier)
                .build();
    }

    public QueuableTask createTask(ResAllocs taskAllocs) {
        return new TestableQueuableTask(taskAllocs.getTaskGroupName(), taskAllocs.getCores(), taskAllocs.getMemory(),
                taskAllocs.getNetworkMbps(), taskAllocs.getDisk()
        );
    }

    public Map<Integer, ResAllocs> getTierCapacities() {
        HashMap<Integer, ResAllocs> result = new HashMap<>();
        tiers.forEach((tidx, state) -> result.put(tidx, state.getTierCapacity()));
        return result;
    }

    public Map<Integer, Map<String, ResAllocs>> getBucketCapacities() {
        HashMap<Integer, Map<String, ResAllocs>> result = new HashMap<>();
        tiers.forEach((tidx, state) -> result.put(tidx, state.getBucketCapacities()));
        return result;
    }

    public TierSla getTierSla(int tierNumber) {
        TierSla tierSla = new TierSla();
        TierState tierState = tiers.get(tierNumber);
        tierSla.setTierCapacity(tierState.getTierCapacity());
        tierState.getBucketCapacities().forEach(tierSla::setAlloc);
        return tierSla;
    }

    public class TierState {
        private final int tierNumber;
        private ResAllocs tierCapacity;
        private Map<String, ResAllocs> bucketCapacities;

        private TierState(int tierNumber, ResAllocs tierCapacity) {
            this.tierNumber = tierNumber;
            this.tierCapacity = tierCapacity;
            this.bucketCapacities = new HashMap<>();
        }

        public int getTierNumber() {
            return tierNumber;
        }

        public ResAllocs getTierCapacity() {
            return tierCapacity;
        }

        public Map<String, ResAllocs> getBucketCapacities() {
            return bucketCapacities;
        }

        private void addBucket(String name, ResAllocs bucketCapacity) {
            bucketCapacities.put(name, ResAllocsUtil.rename(name, bucketCapacity));
        }

        private void removeBucket(String bucketName) {
            if (bucketCapacities.remove(bucketName) == null) {
                throw new IllegalArgumentException("Unknown bucket " + bucketName);
            }
        }

        private void updateTer(ResAllocs tierCapacity) {
            this.tierCapacity = tierCapacity;
        }
    }

    class TestableQueuableTask implements QueuableTask {

        private final String id;
        private final String taskGroupName;
        private final double cpu;
        private final double memory;
        private final double networkMbps;
        private final double disk;
        private final QAttributes qAttributes;
        private AssignedResources assignedResources;

        TestableQueuableTask(String taskGroupName, double cpu, double memory, double networkMbps, double disk) {
            this.id = "task#" + taskIdx++;
            this.taskGroupName = taskGroupName;
            this.cpu = cpu;
            this.memory = memory;
            this.networkMbps = networkMbps;
            this.disk = disk;
            this.qAttributes = new QAttributes() {
                @Override
                public String getBucketName() {
                    return taskGroupName;
                }

                @Override
                public int getTierNumber() {
                    throw new IllegalStateException("method not supported");
                }
            };
        }

        @Override
        public QAttributes getQAttributes() {
            return qAttributes;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String taskGroupName() {
            return taskGroupName;
        }

        @Override
        public double getCPUs() {
            return cpu;
        }

        @Override
        public double getMemory() {
            return memory;
        }

        @Override
        public double getNetworkMbps() {
            return networkMbps;
        }

        @Override
        public double getDisk() {
            return disk;
        }

        @Override
        public int getPorts() {
            return 0;
        }

        @Override
        public Map<String, Double> getScalarRequests() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
            return Collections.emptyMap();
        }

        @Override
        public List<? extends ConstraintEvaluator> getHardConstraints() {
            return Collections.emptyList();
        }

        @Override
        public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
            return Collections.emptyList();
        }

        @Override
        public void setAssignedResources(AssignedResources assignedResources) {
            this.assignedResources = assignedResources;
        }

        @Override
        public AssignedResources getAssignedResources() {
            return assignedResources;
        }
    }
}
