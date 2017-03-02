package com.netflix.fenzo.queues.tiered;

import org.junit.Test;

import java.util.Map;

import static com.netflix.fenzo.queues.tiered.SampleDataGenerator.createResAllocs;
import static com.netflix.fenzo.sla.ResAllocsUtil.hasEqualResources;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TieredQueueSlasTest {

    private static final String BUCKET_0 = "bucket#0";
    private static final String BUCKET_1 = "bucket#1";

    private final SampleDataGenerator generator = new SampleDataGenerator()
            .addTier(0, createResAllocs(8))
            .addBucket(0, BUCKET_0, createResAllocs(4))
            .addBucket(0, BUCKET_1, createResAllocs(4));

    @Test
    public void testSlas() throws Exception {
        Map<Integer, TierSla> slas = new TieredQueueSlas(generator.getTierCapacities(), generator.getBucketCapacities()).getSlas();
        assertThat(slas.size(), is(equalTo(1)));

        TierSla tier0Sla = slas.get(0);
        assertThat(hasEqualResources(tier0Sla.getTierCapacity(), createResAllocs(8)), is(true));
        assertThat(hasEqualResources(tier0Sla.getBucketAllocs(BUCKET_0), createResAllocs(4)), is(true));

        assertThat(tier0Sla.evalAllocationShare(BUCKET_0), is(equalTo(0.5)));
    }
}