package com.netflix.fenzo.plugins;

import java.util.Arrays;
import java.util.List;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.plugins.WeightedAverageFitnessCalculator.WeightedFitnessCalculator;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WeightedAverageFitnessCalculatorTest {

    @Test(expected = IllegalArgumentException.class)
    public void testWeightsMustEqualOne() throws Exception {
        VMTaskFitnessCalculator fitnessCalculator = mock(VMTaskFitnessCalculator.class);
        List<WeightedFitnessCalculator> weightedFitnessCalculators = Arrays.asList(
                new WeightedFitnessCalculator(fitnessCalculator, 0.3),
                new WeightedFitnessCalculator(fitnessCalculator, 0.75)
        );
        new WeightedAverageFitnessCalculator(weightedFitnessCalculators);
    }

    @Test
    public void testCalculatorWeightedAverage() throws Exception {
        VMTaskFitnessCalculator fitnessCalculator1 = mock(VMTaskFitnessCalculator.class);
        when(fitnessCalculator1.calculateFitness(any(), any(), any())).thenReturn(0.5);

        VMTaskFitnessCalculator fitnessCalculator2 = mock(VMTaskFitnessCalculator.class);
        when(fitnessCalculator2.calculateFitness(any(), any(), any())).thenReturn(1.0);

        List<WeightedFitnessCalculator> weightedFitnessCalculators = Arrays.asList(
                new WeightedFitnessCalculator(fitnessCalculator1, 0.5),
                new WeightedFitnessCalculator(fitnessCalculator2, 0.5)
        );
        WeightedAverageFitnessCalculator calculator = new WeightedAverageFitnessCalculator(weightedFitnessCalculators);
        double fitness = calculator.calculateFitness(mock(TaskRequest.class), mock(VirtualMachineCurrentState.class), mock(TaskTrackerState.class));
        Assert.assertEquals(0.75, fitness, 0.0);
    }

    @Test
    public void testCalculatorWeightedAverageWithZero() throws Exception {
        VMTaskFitnessCalculator fitnessCalculator1 = mock(VMTaskFitnessCalculator.class);
        when(fitnessCalculator1.calculateFitness(any(), any(), any())).thenReturn(0.0);

        VMTaskFitnessCalculator fitnessCalculator2 = mock(VMTaskFitnessCalculator.class);
        when(fitnessCalculator2.calculateFitness(any(), any(), any())).thenReturn(1.0);

        List<WeightedFitnessCalculator> weightedFitnessCalculators = Arrays.asList(
                new WeightedFitnessCalculator(fitnessCalculator1, 0.3),
                new WeightedFitnessCalculator(fitnessCalculator2, 0.7)
        );
        WeightedAverageFitnessCalculator calculator = new WeightedAverageFitnessCalculator(weightedFitnessCalculators);
        double fitness = calculator.calculateFitness(mock(TaskRequest.class), mock(VirtualMachineCurrentState.class), mock(TaskTrackerState.class));
        Assert.assertEquals(0.0, fitness, 0.0);
    }
}