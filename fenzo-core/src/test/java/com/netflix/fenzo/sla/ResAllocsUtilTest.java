package com.netflix.fenzo.sla;

import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.tiered.SampleDataGenerator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.netflix.fenzo.sla.ResAllocsUtil.hasEqualResources;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ResAllocsUtilTest {

    private final SampleDataGenerator generator = new SampleDataGenerator();

    @Test
    public void testAdd() throws Exception {
        ResAllocs result = ResAllocsUtil.add(generator.createResAllocs(1), generator.createResAllocs(1));
        assertThat(hasEqualResources(result, generator.createResAllocs(2)), is(true));
    }

    @Test
    public void testSubtract() throws Exception {
        ResAllocs result = ResAllocsUtil.subtract(generator.createResAllocs(2), generator.createResAllocs(1));
        assertThat(hasEqualResources(result, generator.createResAllocs(1)), is(true));
    }

    @Test
    public void testCeilingOf() throws Exception {
        ResAllocs result = ResAllocsUtil.ceilingOf(
                new ResAllocsBuilder("any").withCores(10).withMemory(20).withNetworkMbps(30).withDisk(40).build(),
                new ResAllocsBuilder("any").withCores(5).withMemory(25).withNetworkMbps(15).withDisk(45).build()
        );
        assertThat(hasEqualResources(
                new ResAllocsBuilder("any").withCores(10).withMemory(25).withNetworkMbps(30).withDisk(45).build(),
                result
        ), is(true));
    }

    @Test
    public void testIsBounded() throws Exception {
        ResAllocs reference = generator.createResAllocs(2);
        assertThat(ResAllocsUtil.isBounded(generator.createResAllocs(1), reference), is(true));
        assertThat(ResAllocsUtil.isBounded(generator.createResAllocs(2), reference), is(true));
        assertThat(ResAllocsUtil.isBounded(generator.createResAllocs(3), reference), is(false));

        // Task variant
        QueuableTask taskReference = generator.createTask(reference);
        assertThat(ResAllocsUtil.isBounded(generator.createResAllocs(1), taskReference), is(true));
        assertThat(ResAllocsUtil.isBounded(generator.createResAllocs(2), taskReference), is(true));
        assertThat(ResAllocsUtil.isBounded(generator.createResAllocs(3), taskReference), is(false));
    }

    @Test
    public void testToResAllocs() throws Exception {
        Map<VMResource, Double> resourceMap = new HashMap<>();

        assertThat(hasEqualResources(ResAllocsUtil.toResAllocs("my", resourceMap), ResAllocsUtil.empty()), is(true));
    }
}