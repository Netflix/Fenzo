package com.netflix.fenzo.queues.tiered;

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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TierTest {

    private static final String BUCKET_0 = "bucket#0";
    private static final String BUCKET_1 = "bucket#1";

    private final SampleDataGenerator generator = new SampleDataGenerator()
            .addTier(0, createResAllocs(6))
            .addBucket(0, BUCKET_0, createResAllocs(2))
            .addBucket(0, BUCKET_1, createResAllocs(4));

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
        for (int i = 0; i < 6; i++) {
            QueuableTask next = tier.nextTaskToLaunch();
            scheduledTasks.add(next.getId());
            tier.assignTask(next);
        }
        Set<String> expected = new HashSet<>();
        expected.addAll(queue0Tasks.subList(0, 2));
        expected.addAll(queue1Tasks.subList(0, 4));

        assertThat(scheduledTasks, is(equalTo(expected)));
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
}