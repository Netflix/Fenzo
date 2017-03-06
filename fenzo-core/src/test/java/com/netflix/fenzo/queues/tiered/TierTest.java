package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.queues.Assignable;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueueException;
import com.netflix.fenzo.sla.ResAllocs;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.netflix.fenzo.queues.tiered.SampleDataGenerator.createResAllocs;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TierTest {

    private static final String BUCKET_0 = "bucket#0";
    private static final String BUCKET_1 = "bucket#1";
    private static final String UNUSED_BUCKET = "unusedBucket";
    private static final String BEST_EFFORT_BUCKET = "bestEffortBucket";

    private final SampleDataGenerator generator = new SampleDataGenerator()
            .addTier(0, createResAllocs(10))
            .addBucket(0, BUCKET_0, createResAllocs(2))
            .addBucket(0, BUCKET_1, createResAllocs(4))
            .addBucket(0, UNUSED_BUCKET, createResAllocs(4));

    private final BiFunction<Integer, String, Double> allocsShareGetter = mock(BiFunction.class);

    private final Tier tier = new Tier(0, allocsShareGetter);

    @Before
    public void setUp() throws Exception {
        when(allocsShareGetter.apply(anyInt(), anyString())).thenReturn(1.0);
        tier.setTierSla(generator.getTierSla(0));
    }

    @Test
    public void testTaskAreLaunchedInCorrectOrder() throws Exception {
        List<String> queue0Tasks = queue(5, createResAllocs(BUCKET_0, 1));
        List<String> queue1Tasks = queue(5, createResAllocs(BUCKET_1, 1));

        Set<String> scheduledTasks = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Assignable<QueuableTask> taskOrFailure = tier.nextTaskToLaunch();
            if (!taskOrFailure.hasFailure()) {
                QueuableTask next = taskOrFailure.getTask();
                scheduledTasks.add(next.getId());
                tier.assignTask(next);
            }
        }
        Set<String> expected = new HashSet<>();
        expected.addAll(queue0Tasks.subList(0, 2));
        expected.addAll(queue1Tasks.subList(0, 4));

        assertThat(scheduledTasks, is(equalTo(expected)));

        // Anything left is kept for the guaranteed capacity
        assertThat(tier.nextTaskToLaunch(), is(nullValue()));
    }

    @Test
    public void testTasksInQueueWithoutSlaConsumeRemainingCapacityOnly() throws Exception {
        // Add extra capacity
        generator.updateTier(0, createResAllocs(12));
        tier.setTierSla(generator.getTierSla(0));

        // Take as much as you can for best effort tasks
        queue(10, createResAllocs(BEST_EFFORT_BUCKET, 1));
        int counter = consumeAll();
        assertThat(counter, is(equalTo(2)));

        // Now schedule task from the guaranteed queue
        tier.reset();
        List<String> queue0Tasks = queue(5, createResAllocs(BUCKET_0, 1));
        for (int i = 0; i < 2; i++) {
            Assignable<QueuableTask> next = tier.nextTaskToLaunch();
            assertThat(next, is(notNullValue()));
            assertThat(next.hasFailure(), is(false));
            assertThat(queue0Tasks.contains(next.getTask().getId()), is(true));
            tier.assignTask(next.getTask());
        }
    }

    /**
     * If a queue is removed, and it had SLA defined, we should no longer consider it. As a result a remaining/left
     * over capacity should be increased by this amount.
     */
    @Test
    public void testRemovingQueueWithGuaranteesReleasesItsResources() throws Exception {
        queue(10, createResAllocs(BEST_EFFORT_BUCKET, 1));

        // No spare capacity == nothing queued
        assertThat(consumeAll(), is(equalTo(0)));

        // Release bucket
        generator.removeBucket(0, BUCKET_0);
        tier.setTierSla(generator.getTierSla(0));
        tier.reset();

        assertThat(consumeAll(), is(equalTo(2)));

    }

    private List<String> queue(int numberOfTasks, ResAllocs resAllocs) throws TaskQueueException {
        List<String> taskids = new ArrayList<>(numberOfTasks);
        for (int i = 0; i < numberOfTasks; i++) {
            QueuableTask task = generator.createTask(resAllocs);
            tier.queueTask(task);
            taskids.add(task.getId());
        }
        return taskids;
    }

    private int consumeAll() throws TaskQueueException {
        int counter = 0;
        Assignable<QueuableTask> taskOrFailure;
        while ((taskOrFailure = tier.nextTaskToLaunch()) != null) {
            if (!taskOrFailure.hasFailure()) {
                tier.assignTask(taskOrFailure.getTask());
                counter++;
            }
        }
        return counter;
    }
}