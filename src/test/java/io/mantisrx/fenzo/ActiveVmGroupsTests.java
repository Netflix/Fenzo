package io.mantisrx.fenzo;

import junit.framework.Assert;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActiveVmGroupsTests {
    private static final String activeVmGrpAttrName = "ASG";
    private static final String activeVmGrp = "test1";
    private TaskScheduler taskScheduler;
    private final Map<String, Protos.Attribute> attributes1 = new HashMap<>();
    private final Map<String, Protos.Attribute> attributes2 = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        taskScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Rejecting offer on host " + virtualMachineLease.hostname());
                    }
                })
                .build();
        taskScheduler.setActiveVmGroupAttributeName(activeVmGrpAttrName);
        taskScheduler.setActiveVmGroups(Arrays.asList(activeVmGrp));
        Protos.Attribute attribute1 = Protos.Attribute.newBuilder().setName(activeVmGrpAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue("test1")).build();
        attributes1.put(activeVmGrpAttrName, attribute1);
        Protos.Attribute attribute2 = Protos.Attribute.newBuilder().setName(activeVmGrpAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue("test2")).build();
        attributes2.put(activeVmGrpAttrName, attribute2);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInactiveVmGroup() {
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        List<VirtualMachineLease> leases = Arrays.asList(LeaseProvider.getLeaseOffer("host1", 4, 4000, ports, attributes2));
        List<TaskRequest> tasks = Arrays.asList(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        Map<String, VMAssignmentResult> resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertEquals(0, resultMap.size());
        leases = Arrays.asList(LeaseProvider.getLeaseOffer("host2", 4, 4000, ports, attributes1));
        resultMap = taskScheduler.scheduleOnce(tasks, leases).getResultMap();
        Assert.assertEquals(1, resultMap.size());
        Assert.assertEquals(tasks.get(0).getId(), resultMap.values().iterator().next().getTasksAssigned().iterator().next().getTaskId());
    }
}
