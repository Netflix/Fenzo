package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.UsageTrackedQueue.ResUsage;
import com.netflix.fenzo.sla.ResAllocs;
import org.junit.Test;

import java.util.function.BiFunction;

import static com.netflix.fenzo.queues.tiered.FenzoMatchers.hasEqualResources;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueueBucketTest {

    private static final String BUCKET_NAME = "bucket#0";

    private final SampleDataGenerator generator = new SampleDataGenerator();

    private final ResUsage tierUsage = new ResUsage();

    private final BiFunction<Integer, String, Double> allocsShareGetter = mock(BiFunction.class);

    private final QueueBucket queueBucket = new QueueBucket(0, BUCKET_NAME, tierUsage, allocsShareGetter);

    @Test
    public void testBucketGuaranteesAffectEffectiveUsageComputation() throws Exception {
        ResAllocs bucketCapacity = generator.createResAllocs(BUCKET_NAME, 2);
        queueBucket.setBucketGuarantees(bucketCapacity);

        // No usage
        assertThat(hasEqualResources(queueBucket.getEffectiveUsage(), bucketCapacity), is(true));
        assertThat(queueBucket.isBelowGuaranteedCapacity(), is(true));

        // Half usage
        queueBucket.launchTask(generator.createTask(generator.createResAllocs("task#0", 1)));
        assertThat(hasEqualResources(queueBucket.getEffectiveUsage(), bucketCapacity), is(true));
        assertThat(queueBucket.isBelowGuaranteedCapacity(), is(true));

        // Full bucket usage
        queueBucket.launchTask(generator.createTask(generator.createResAllocs("task#1", 1)));
        assertThat(hasEqualResources(queueBucket.getEffectiveUsage(), bucketCapacity), is(true));
        assertThat(queueBucket.isBelowGuaranteedCapacity(), is(false));

        // Above bucket usage
        queueBucket.launchTask(generator.createTask(generator.createResAllocs("task#2", 1)));
        assertThat(hasEqualResources(queueBucket.getEffectiveUsage(), generator.createResAllocs(3)), is(true));
        assertThat(queueBucket.isBelowGuaranteedCapacity(), is(false));
    }

    @Test
    public void testDominantUsageShare() throws Exception {
        when(allocsShareGetter.apply(anyInt(), anyString())).thenReturn(1.0);

        tierUsage.addUsage(generator.createTask(generator.createResAllocs(4)));

        // Half-fill the bucket, so share == 1/4 of current tier usage
        ResAllocs bucketCapacity = generator.createResAllocs(BUCKET_NAME, 2);
        queueBucket.setBucketGuarantees(bucketCapacity);

        QueuableTask task = generator.createTask(generator.createResAllocs("task#0", 1));
        queueBucket.launchTask(task);

        assertThat(queueBucket.getDominantUsageShare(), is(equalTo(0.25)));

        // Set tier capacity explicitly to be twice as much as its current usage
        queueBucket.setTotalResources(generator.createResAllocs(8));
        assertThat(queueBucket.getDominantUsageShare(), is(equalTo(0.125)));
    }
}
