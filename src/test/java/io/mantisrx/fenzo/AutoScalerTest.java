package io.mantisrx.fenzo;

import io.mantisrx.fenzo.plugins.BinPackingFitnessCalculators;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AutoScalerTest {

    static String hostAttrName = "MachineType";
    final int minIdle=5;
    final int maxIdle=10;
    final long coolDownSecs=5;
    String hostAttrVal1="4coreServers";
    String hostAttrVal2="8coreServers";
    int cpus1=4;
    int memory1=40000;
    int cpus2=8;
    int memory2=800;  // make this less than memory1/cpus1 to ensure jobs don't get on these
    final AutoScaleRule rule1 = AutoScaleRuleProvider.createRule(hostAttrVal1, minIdle, maxIdle, coolDownSecs, cpus1/2, memory1/2);
    final AutoScaleRule rule2 = AutoScaleRuleProvider.createRule(hostAttrVal2, minIdle, maxIdle, coolDownSecs, cpus2/2, memory2/2);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    private TaskScheduler getScheduler(AutoScaleRule... rules) {
        return getScheduler(false, rules);
    }
    private TaskScheduler getScheduler(final boolean expectLeaseRejection, AutoScaleRule... rules) {
        TaskScheduler.Builder builder = new TaskScheduler.Builder()
                .withAutoScaleByAttributeName(hostAttrName);
        for(AutoScaleRule rule: rules)
            builder.withAutoScaleRule(rule);
        return builder
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuBinPacker)
                .withLeaseOfferExpirySecs(3600)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        if(!expectLeaseRejection)
                            Assert.fail("Unexpected to reject lease " + lease.hostname());
                    }
                })
                .build();
    }

    // Test autoscale up on a simple rule
    // - Setup an auto scale rule
    // - keep calling scheduleOnce() periodically until a time greater than autoscale rule's cooldown period
    // - ensure that we get a scale up action within the time
    @Test
    public void scaleUpTest1() throws Exception {
        TaskScheduler scheduler = getScheduler(rule1);
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger scaleUpRequest = new AtomicInteger(0);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleUpAction) {
                            int needed = ((ScaleUpAction)autoScaleAction).getScaleUpCount();
                            scaleUpRequest.set(needed);
                            latch.countDown();
                        }
                    }
                })
                .subscribe();
        List<TaskRequest> requests = new ArrayList<>();
        int i=0;
        do {
            Thread.sleep(1000);
            scheduler.scheduleOnce(requests, leases);
        } while (i++<(coolDownSecs+2) && latch.getCount()>0);
        if(latch.getCount()>0)
            Assert.fail("Timed out scale up action");
        else
            Assert.assertEquals(maxIdle, scaleUpRequest.get());
    }

    // Test scale up action repeating after using up all hosts after first scale up action
    // Setup an auto scale rule
    // start with 0 hosts available
    // keep calling TaskScheduler.scheduleOnce()
    // ensure we get scale up action
    // on first scale up action add some machines
    // use up all of those machines in subsequent scheduling
    // ensure we get another scale up action after cool down time
    @Test
    public void scaleUpTest2() throws Exception {
        TaskScheduler scheduler = getScheduler(rule1);
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean addVMs = new AtomicBoolean(false);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final List<TaskRequest> requests = new ArrayList<>();
        for(int i=0; i<maxIdle*cpus1; i++)
            requests.add(TaskRequestProvider.getTaskRequest(1.0, 100, 1));
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleUpAction) {
                            if(!addVMs.compareAndSet(false, true)) {
                                // second time around here
                                latch.countDown();
                            }
                        }
                    }
                })
                .subscribe();
        int i=0;
        boolean added=false;
        do {
            Thread.sleep(1000);
            if(!added && addVMs.get()) {
                leases.addAll(LeaseProvider.getLeases(maxIdle, cpus1, memory1, 1, 10));
                added=true;
            }
            SchedulingResult schedulingResult = scheduler.scheduleOnce(requests, leases);
            Map<String,VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            if(added) {
                int count=0;
                for(VMAssignmentResult result: resultMap.values())
                    count += result.getTasksAssigned().size();
                Assert.assertEquals(requests.size(), count);
                requests.clear();
                leases.clear();
            }
        } while (i++<(2*coolDownSecs+2) && latch.getCount()>0);
        Assert.assertTrue("Second scale up action didn't arrive on time", latch.getCount()==0);
    }

    /**
     * Tests that the rule applies only to the host types specified and not to the other host type.
     * @throws Exception upon any error
     */
    @Test
    public void testOneRuleTwoTypes() throws Exception {
        TaskScheduler scheduler = getScheduler(rule2);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final List<TaskRequest> requests = new ArrayList<>();
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        for(int i=0; i<maxIdle*cpus1; i++)
            requests.add(TaskRequestProvider.getTaskRequest(1.0, 100, 1));
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleUpAction) {
                            if(autoScaleAction.getRuleName().equals(rule1.getRuleName()))
                                latch.countDown();
                        }
                    }
                })
                .subscribe();
        int i=0;
        do {
            Thread.sleep(1000);
            scheduler.scheduleOnce(requests, leases);
        } while(i++<coolDownSecs+2 && latch.getCount()>0);
        if(latch.getCount()<1)
            Assert.fail("Should not have gotten scale up action for " + rule1.getRuleName());
    }

    /**
     * Tests that of the two AutoScale rules setup, scale up action is called only on the one that is actually short.
     * @throws Exception
     */
    @Test
    public void testTwoRulesOneNeedsScaleUp() throws Exception {
        TaskScheduler scheduler = getScheduler(rule1, rule2);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final List<TaskRequest> requests = new ArrayList<>();
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleUpAction) {
                            if(autoScaleAction.getRuleName().equals(rule2.getRuleName()))
                                latch.countDown();
                        }
                    }
                })
                .subscribe();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal2)).build();
        attributes.put(hostAttrName, attribute);
        for(int l=0; l<maxIdle; l++) {
            leases.add(LeaseProvider.getLeaseOffer("host"+l, 8, 8000, ports, attributes));
        }
        int i=0;
        do {
            Thread.sleep(1000);
            scheduler.scheduleOnce(requests, leases);
        } while (i++<coolDownSecs+2 && latch.getCount()>0);
        if(latch.getCount()<1)
            Assert.fail("Scale up action received for " + rule2.getRuleName() + " rule, was expecting only on "
                    + rule1.getRuleName());
    }

    /**
     * Tests simple scale down action on host type that has excess capacity
     * @throws Exception
     */
    @Test
    public void testSimpleScaleDownAction() throws Exception {
        final AtomicInteger scaleDownCount = new AtomicInteger();
        TaskScheduler scheduler = getScheduler(true, rule1);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final List<TaskRequest> requests = new ArrayList<>();
        for(int c=0; c<cpus1; c++) // add as many 1-CPU requests as #cores on a host
            requests.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleDownAction) {
                            scaleDownCount.set(((ScaleDownAction) autoScaleAction).getHosts().size());
                            latch.countDown();
                        }
                    }
                })
                .subscribe();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1)).build();
        attributes.put(hostAttrName, attribute);
        int excess=3;
        for(int l=0; l<maxIdle+excess; l++) {
            leases.add(LeaseProvider.getLeaseOffer("host"+l, cpus1, memory1, ports, attributes));
        }
        int i=0;
        boolean first=true;
        do {
            Thread.sleep(1000);
            scheduler.scheduleOnce(requests, leases);
            if(first) {
                first=false;
                leases.clear();
                requests.clear();
            }
        } while(i++<coolDownSecs+2 && latch.getCount()>0);
        Assert.assertEquals(0, latch.getCount());
        // expect scale down count to be excess-1 since we used up 1 host
        Assert.assertEquals(excess-1, scaleDownCount.get());
    }

    /**
     * Tests that of the two rules, scale down is called only on the one that is in excess
     * @throws Exception
     */
    @Test
    public void testTwoRuleScaleDownAction() throws Exception {
        TaskScheduler scheduler = getScheduler(true, rule1, rule2);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final List<TaskRequest> requests = new ArrayList<>();
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final String wrongScaleDownRulename = rule1.getRuleName();
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleDownAction) {
                            if(autoScaleAction.getRuleName().equals(wrongScaleDownRulename))
                                latch.countDown();
                        }
                    }
                })
                .subscribe();
        // use up servers covered by rule1
        for(int r=0; r<maxIdle*cpus1; r++)
            requests.add(TaskRequestProvider.getTaskRequest(1, memory1/cpus1, 1));
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        Map<String, Protos.Attribute> attributes1 = new HashMap<>();
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1)).build();
        attributes1.put(hostAttrName, attribute);
        Map<String, Protos.Attribute> attributes2 = new HashMap<>();
        Protos.Attribute attribute2 = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal2)).build();
        attributes2.put(hostAttrName, attribute2);
        for(int l=0; l<maxIdle+3; l++) {
            leases.add(LeaseProvider.getLeaseOffer("host"+l, cpus1, memory1, ports, attributes1));
            leases.add(LeaseProvider.getLeaseOffer("host"+100+l, cpus2, memory2, ports, attributes2));
        }
        int i=0;
        boolean first=true;
        do {
            Thread.sleep(1000);
            scheduler.scheduleOnce(requests, leases);
            if(first) {
                first=false;
                requests.clear();
                leases.clear();
            }
        } while (i++<coolDownSecs+2 && latch.getCount()>0);
        if(latch.getCount()<1)
            Assert.fail("Scale down action received for " + wrongScaleDownRulename + " rule, was expecting only on "
                    + rule1.getRuleName());
    }

    // Tests that when scaling down, a balance is achieved across hosts for the given attribute. That is, about equal
    // number of hosts remain after scale down, for each unique value of the given attribute. Say we are trying to
    // balance the number of hosts across the zone attribute, then, after scale down there must be equal number of
    // hosts for each zone.
    @Test
    public void testScaleDownBalanced() throws Exception {
        final String zoneAttrName="Zone";
        final int mxIdl=12;
        final CountDownLatch latch = new CountDownLatch(1);
        final int[] zoneCounts = {0, 0, 0};
        final List<TaskRequest> requests = new ArrayList<>();
        // add enough jobs to fill two machines of zone 0
        List<ConstraintEvaluator> hardConstraints = new ArrayList<>();
        hardConstraints.add(ConstraintsProvider.getHostAttributeHardConstraint(zoneAttrName, ""+1));
        for(int j=0; j<cpus1*2; j++) {
            requests.add(TaskRequestProvider.getTaskRequest(1, memory1/cpus1, 1, hardConstraints, Collections.EMPTY_LIST));
        }
        final List<VirtualMachineLease> leases = new ArrayList<>();
        final AutoScaleRule rule = AutoScaleRuleProvider.createRule(hostAttrVal1, 3, mxIdl, coolDownSecs, cpus1/2, memory1/2);
        final TaskScheduler scheduler = new TaskScheduler.Builder()
                .withAutoScaleByAttributeName(hostAttrName)
                .withAutoScaleDownBalancedByAttributeName(zoneAttrName)
                .withFitnessCalculator(BinPackingFitnessCalculators.cpuBinPacker)
                .withLeaseOfferExpirySecs(3600)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        //System.out.println("Rejecting lease on " + lease.hostname());
                    }
                })
                .withAutoScaleRule(rule)
                .build();
        scheduler.getAutoScaleActionsObservable()
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        //System.out.println("autoscale action type: " + autoScaleAction.getType() + " ruleName=" + autoScaleAction.getRuleName());
                        if (autoScaleAction.getType() == AutoScaleAction.Type.Down) {
                            final Collection<String> hosts = ((ScaleDownAction) autoScaleAction).getHosts();
                            for (String h : hosts) {
                                int zoneNum = Integer.parseInt(h.substring("host".length())) % 3;
                                //System.out.println("Scaling down host " + h);
                                zoneCounts[zoneNum]--;
                            }
                            latch.countDown();
                        }
                        else
                            Assert.fail("Wasn't expecting to scale up");
                    }
                })
                .subscribe();
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        // create three attributes, each with unique zone value
        Map<String, Protos.Attribute>[] attributes = new HashMap[3];
        Protos.Attribute attr = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1)).build();
        for(int i=0; i<attributes.length; i++) {
            attributes[i] = new HashMap<>();
            Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(zoneAttrName)
                    .setType(Protos.Value.Type.TEXT)
                    .setText(Protos.Value.Text.newBuilder().setValue(""+i)).build();
            attributes[i].put(zoneAttrName, attribute);
            attributes[i].put(hostAttrName, attr);
        }
        for(int l=0; l<mxIdl+6; l++) {
            final int zoneNum = l % 3;
            leases.add(LeaseProvider.getLeaseOffer("host"+l, cpus1, memory1, ports, attributes[zoneNum]));
            zoneCounts[zoneNum]++;
        }
        int i=0;
        boolean first=true;
        do {
            Thread.sleep(1000);
            final SchedulingResult schedulingResult = scheduler.scheduleOnce(requests, leases);
//            System.out.println("idleSlaves#: " + schedulingResult.getIdleSlavesCount() + ", #leasesAdded=" +
//                    schedulingResult.getLeasesAdded() + ", #totalSlaves=" + schedulingResult.getTotalSlavesCount());
//            System.out.println("#leasesRejected=" + schedulingResult.getLeasesRejected());
            final Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            for(Map.Entry<String, VMAssignmentResult> entry: resultMap.entrySet()) {
                final int zn = Integer.parseInt(entry.getValue().getLeasesUsed().get(0).getAttributeMap().get(zoneAttrName).getText().getValue());
                zoneCounts[zn]--;
            }
            if(first) {
                first=false;
                requests.clear();
                leases.clear();
            }
        } while (i++<coolDownSecs+2 && latch.getCount()>0);
        if(latch.getCount()>0)
            Assert.fail("Didn't get scale down");
        for(int z=0; z<zoneCounts.length; z++) {
            Assert.assertEquals(4, zoneCounts[z]);
        }
    }

    /**
     * Test that a scaled down host doesn't get used in spite of receiving an offer for it
     * @throws Exception
     */
    @Test
    public void testScaledDownHostOffer() throws Exception {
        TaskScheduler scheduler = getScheduler(true, rule1);
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Collection<String>> scaleDownHostsRef = new AtomicReference<>();
        final List<TaskRequest> requests = new ArrayList<>();
        final List<VirtualMachineLease> leases = new ArrayList<>();
        for(int j=0; j<cpus1; j++) // fill one machine
            requests.add(TaskRequestProvider.getTaskRequest(1, memory1/cpus1, 1));
        List<VirtualMachineLease.Range> ports = new ArrayList<>();
        ports.add(new VirtualMachineLease.Range(1, 10));
        Map<String, Protos.Attribute> attributes1 = new HashMap<>();
        Protos.Attribute attribute = Protos.Attribute.newBuilder().setName(hostAttrName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(hostAttrVal1)).build();
        attributes1.put(hostAttrName, attribute);
        int excess=3;
        for(int l=0; l<maxIdle+excess; l++) {
            leases.add(LeaseProvider.getLeaseOffer("host"+l, cpus1, memory1, ports, attributes1));
        }
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleDownAction) {
                            scaleDownHostsRef.set(((ScaleDownAction) autoScaleAction).getHosts());
                            latch.countDown();
                        }
                    }
                })
                .subscribe();
        int i=0;
        boolean first=true;
        do {
            Thread.sleep(1000);
            scheduler.scheduleOnce(requests, leases);
            if(first) {
                first=false;
                requests.clear();
                leases.clear();
            }
        } while (i++<coolDownSecs+2 && latch.getCount()>0);
        Assert.assertEquals(0, latch.getCount());
        // remove any existing leases in scheduler
        // now generate offers for hosts that were scale down and ensure they don't get used
        for(String hostname: scaleDownHostsRef.get()) {
            leases.add(LeaseProvider.getLeaseOffer(hostname, cpus1, memory1, ports, attributes1));
        }
        // now try to fill all machines minus one that we filled before
        for(int j=0; j<(maxIdle+excess-1)*cpus1; j++)
            requests.add(TaskRequestProvider.getTaskRequest(1, memory1/cpus1, 1));
        i=0;
        first=true;
        do {
            Thread.sleep(1000);
            SchedulingResult schedulingResult = scheduler.scheduleOnce(requests, leases);
            if(!schedulingResult.getResultMap().isEmpty()) {
                for(Map.Entry<String, VMAssignmentResult> entry: schedulingResult.getResultMap().entrySet()) {
                    Assert.assertFalse("Did not expect scaled down host " + entry.getKey() + " to be assigned again",
                            isInCollection(entry.getKey(), scaleDownHostsRef.get()));
                    for(int j=0; j<entry.getValue().getTasksAssigned().size(); j++)
                        requests.remove(0);
                }
            }
            if(first) {
                leases.clear();
            }
        } while(i++<coolDownSecs-1);
    }

    // Tests that resource shortfall is evaluated and scale up happens beyond what would otherwise request only up to
    // maxIdle count for the scaling rule. Also, that scale up from shortfall due to new tasks doesn't wait for cooldown
    @Test
    public void testResourceShortfall() throws Exception {
        TaskScheduler scheduler = getScheduler(true, AutoScaleRuleProvider.createRule(hostAttrVal1, minIdle, maxIdle, coolDownSecs, 1, 1000));
        Observable<AutoScaleAction> autoScaleActionsObservable = scheduler.getAutoScaleActionsObservable();
        final List<TaskRequest> requests = new ArrayList<>();
        final List<VirtualMachineLease> leases = new ArrayList<>();
        for(int i=0; i<rule1.getMaxIdleHostsToKeep()*2; i++)
            requests.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        leases.addAll(LeaseProvider.getLeases(2, 1, 1000, 1, 10));
        final AtomicInteger scaleUpRequested = new AtomicInteger();
        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(new CountDownLatch(1));
        autoScaleActionsObservable
                .doOnNext(new Action1<AutoScaleAction>() {
                    @Override
                    public void call(AutoScaleAction autoScaleAction) {
                        if(autoScaleAction instanceof ScaleUpAction) {
                            scaleUpRequested.set(((ScaleUpAction)autoScaleAction).getScaleUpCount());
                            latchRef.get().countDown();
                        }
                    }
                })
                .subscribe();
        SchedulingResult schedulingResult = scheduler.scheduleOnce(requests.subList(0, leases.size()), leases);
        Assert.assertNotNull(schedulingResult);
        Thread.sleep(coolDownSecs*1000+1000);
        schedulingResult = scheduler.scheduleOnce(requests.subList(leases.size(), requests.size()), new ArrayList<VirtualMachineLease>());
        Assert.assertNotNull(schedulingResult);
        boolean waitSuccessful = latchRef.get().await(coolDownSecs, TimeUnit.SECONDS);
        Assert.assertTrue(waitSuccessful);
        final int scaleUp = scaleUpRequested.get();
        Assert.assertEquals(requests.size()-leases.size(), scaleUp);
        requests.clear();
        final int newRequests = rule1.getMaxIdleHostsToKeep() * 3;
        for(int i=0; i<newRequests; i++)
            requests.add(TaskRequestProvider.getTaskRequest(1, 1000, 1));
        latchRef.set(new CountDownLatch(1));
        schedulingResult = scheduler.scheduleOnce(requests, new ArrayList<VirtualMachineLease>());
        Assert.assertNotNull(schedulingResult);
        waitSuccessful = latchRef.get().await(coolDownSecs, TimeUnit.SECONDS);
        Assert.assertTrue(waitSuccessful);
        Assert.assertEquals(newRequests, scaleUpRequested.get());
    }

    private boolean isInCollection(String host, Collection<String> hostList) {
        for(String h: hostList)
            if(h.equals(host))
                return true;
        return false;
    }
}
