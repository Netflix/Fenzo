package com.netflix.fenzo;

import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;

public class NaiveShortfallEvaluatorTest {

    private static final String HOST_ATTR_VAL = "4coreServers";
    private static final String ADJUSTED_HOST_ATTR_VAL = "adjusted4coreServers";

    private static final int CPU = 4;
    private static final int MEMORY = 8 * 1024;

    private static final Set<String> ATTR_KEYS = new HashSet<>(asList(HOST_ATTR_VAL, ADJUSTED_HOST_ATTR_VAL));

    private static final AutoScaleRule STD_RULE = AutoScaleRuleProvider.createRule(
            HOST_ATTR_VAL, 0, 10, 600, CPU / 2, MEMORY / 2
    );

    private static final AutoScaleRule ADJUSTED_RULE = AutoScaleRuleProvider.createRule(
            ADJUSTED_HOST_ATTR_VAL, 0, 10, 600, CPU / 2, MEMORY / 2,
            numberOfAgents -> numberOfAgents / 2
    );

    private final AutoScaleRules autoScaleRules = new AutoScaleRules(asList(STD_RULE, ADJUSTED_RULE));

    private final TaskScheduler taskScheduler = mock(TaskScheduler.class);

    private final ShortfallEvaluator shortfallEvaluator = new NaiveShortfallEvaluator();

    @Before
    public void setUp() throws Exception {
        shortfallEvaluator.setTaskToClustersGetter(task ->
                singletonList(task.getId().contains("adjusted") ? ADJUSTED_HOST_ATTR_VAL : HOST_ATTR_VAL)
        );
    }

    @Test
    public void testShortfallAdjusting() throws Exception {
        Set<TaskRequest> failedTasks = new HashSet<>(asList(
                createFailedTask("std#1"), createFailedTask("std#2"),
                createFailedTask("adjusted#1"), createFailedTask("adjusted#2")
        ));
        Map<String, Integer> shortfall = shortfallEvaluator.getShortfall(ATTR_KEYS, failedTasks, autoScaleRules);

        Assert.assertThat(shortfall.get(HOST_ATTR_VAL), is(equalTo(2)));
        Assert.assertThat(shortfall.get(ADJUSTED_HOST_ATTR_VAL), is(equalTo(1)));
    }

    private QueuableTask createFailedTask(String id) {
        return new QueuableTask() {
            @Override
            public String getId() {
                return id;
            }

            @Override
            public String taskGroupName() {
                return null;
            }

            @Override
            public double getCPUs() {
                return CPU / 4;
            }

            @Override
            public double getMemory() {
                return MEMORY / 4;
            }

            @Override
            public double getNetworkMbps() {
                return 0;
            }

            @Override
            public double getDisk() {
                return 0;
            }

            @Override
            public int getPorts() {
                return 0;
            }

            @Override
            public Map<String, Double> getScalarRequests() {
                return null;
            }

            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return null;
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }

            @Override
            public void setAssignedResources(AssignedResources assignedResources) {

            }

            @Override
            public AssignedResources getAssignedResources() {
                return null;
            }

            @Override
            public QAttributes getQAttributes() {
                return null;
            }
        };
    }
}