/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.fenzo;

import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Func1;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class OfferRejectionsTest {

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private TaskScheduler getScheduler(
            long offerExpirySecs, final long leaseReOfferDelaySecs,
            int maxOffersToReject,
            final BlockingQueue<VirtualMachineLease> offersQ,
            final Func1<String, VirtualMachineLease> offerGenerator
    ) {
        return new TaskScheduler.Builder()
                .withLeaseRejectAction(
                        new Action1<VirtualMachineLease>() {
                            @Override
                            public void call(final VirtualMachineLease virtualMachineLease) {
                                executorService.schedule(
                                        new Runnable() {
                                            @Override
                                            public void run() {
                                                offersQ.offer(offerGenerator.call(virtualMachineLease.hostname()));
                                            }
                                        },
                                        leaseReOfferDelaySecs, TimeUnit.SECONDS);
                            }
                        }
                )
                .withLeaseOfferExpirySecs(offerExpirySecs)
                .withMaxOffersToReject(maxOffersToReject)
                .build();
    }

    // Test that offers are being rejected by scheduler based on configured offer expiry
    @Test
    public void testOffersAreRejected() throws Exception {
        final BlockingQueue<VirtualMachineLease> offers = new LinkedBlockingQueue<>();
        long offerExpirySecs=2;
        long leaseReOfferDelaySecs=1;
        final AtomicInteger offersGenerated = new AtomicInteger(0);
        final TaskScheduler scheduler = getScheduler(
                offerExpirySecs, leaseReOfferDelaySecs, 4, offers,
                new Func1<String, VirtualMachineLease>() {
                    @Override
                    public VirtualMachineLease call(String s) {
                        offersGenerated.incrementAndGet();
                        return LeaseProvider.getLeaseOffer(s, 4, 4000, 1, 10);
                    }
                }
        );
        for(int i=0; i<3; i++)
            offers.offer(LeaseProvider.getLeaseOffer("host" + i, 4, 4000, 1, 10));
        for(int i=0; i<offerExpirySecs+leaseReOfferDelaySecs+2; i++) {
            List<VirtualMachineLease> newOffers = new ArrayList<>();
            offers.drainTo(newOffers);
            scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), newOffers);
            Thread.sleep(1000);
        }
        Assert.assertTrue("No offer rejections occured", offersGenerated.get()>0);
    }

    // test that not more than configured number of offers are rejected per configured offer expiry time interval
    @Test
    public void testOffersRejectLimit() throws Exception {
        final BlockingQueue<VirtualMachineLease> offers = new LinkedBlockingQueue<>();
        long offerExpirySecs=3;
        long leaseReOfferDelaySecs=2;
        int maxOffersToReject=2;
        int numHosts=10;
        final AtomicInteger offersGenerated = new AtomicInteger(0);
        final TaskScheduler scheduler = getScheduler(
                offerExpirySecs, leaseReOfferDelaySecs, maxOffersToReject, offers,
                new Func1<String, VirtualMachineLease>() {
                    @Override
                    public VirtualMachineLease call(String s) {
                        offersGenerated.incrementAndGet();
                        return LeaseProvider.getLeaseOffer(s, 4, 4000, 1, 10);
                    }
                }
        );
        for(int i=0; i<numHosts; i++)
            offers.offer(LeaseProvider.getLeaseOffer("host" + i, 4, 4000, 1, 10));
        for(int i=0; i<2*(offerExpirySecs+leaseReOfferDelaySecs+2); i++) {
            List<VirtualMachineLease> newOffers = new ArrayList<>();
            offers.drainTo(newOffers);
            final SchedulingResult result = scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), newOffers);
            int minIdleHosts = numHosts-maxOffersToReject;
            Assert.assertTrue("Idle #hosts should be >= " + minIdleHosts + ", but is " + result.getIdleVMsCount(),
                    result.getIdleVMsCount() >= minIdleHosts);
            Thread.sleep(500);
        }
        Assert.assertTrue("Never rejected any offers", offersGenerated.get()>0);
    }

    // Test that an offer that is on a host that has another task running form before is considered for expiring.
    @Test
    public void testPartialOfferReject() throws Exception {
        long offerExpirySecs=3;
        long leaseReOfferDelaySecs=2;
        int maxOffersToReject=3;
        int numHosts=3;
        final ConcurrentMap<String, String> hostsRejectedFrom = new ConcurrentHashMap<>();
        final TaskScheduler scheduler = new TaskScheduler.Builder()
                .withLeaseRejectAction(
                        new Action1<VirtualMachineLease>() {
                            @Override
                            public void call(final VirtualMachineLease virtualMachineLease) {
                                hostsRejectedFrom.putIfAbsent(virtualMachineLease.hostname(), virtualMachineLease.hostname());
                            }
                        }
                )
                .withLeaseOfferExpirySecs(offerExpirySecs)
                .withMaxOffersToReject(maxOffersToReject)
                .build();
        final SchedulingResult result = scheduler.scheduleOnce(
                Collections.singletonList(TaskRequestProvider.getTaskRequest(1, 1000, 1)),
                Collections.singletonList(LeaseProvider.getLeaseOffer("host0", 4, 4000, 1, 10))
        );
        final Map<String, VMAssignmentResult> resultMap = result.getResultMap();
        Assert.assertEquals(1, resultMap.size());
        final TaskRequest taskRequest = resultMap.values().iterator().next().getTasksAssigned().iterator().next().getRequest();
        scheduler.getTaskAssigner().call(taskRequest, "host0");
        final String assignedHost = result.getResultMap().keySet().iterator().next();
        final VirtualMachineLease consumedLease = LeaseProvider.getConsumedLease(result.getResultMap().values().iterator().next());
        List<VirtualMachineLease> leases = new ArrayList<>();
        // add back offer with remaining resources on first host
        leases.add(consumedLease);
        // add new offers from rest of the hosts
        for(int i=1; i<numHosts; i++)
            leases.add(LeaseProvider.getLeaseOffer("host" + i, 4, 4000, 1, 10));
        for(int i=0; i<(offerExpirySecs+leaseReOfferDelaySecs); i++) {
            scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), leases);
            leases.clear();
            Thread.sleep(1000L);
        }
        System.out.println("assigned hosts: " + assignedHost + ", rejectedFrom: " + hostsRejectedFrom);
        Assert.assertTrue(hostsRejectedFrom.containsKey(assignedHost));
    }

    // test that an expired lease doesn't get used for allocation to a task
    @Test
    public void testExpiryOfLease() throws Exception {
        final AtomicInteger expireCount = new AtomicInteger();
        final TaskScheduler scheduler = new TaskScheduler.Builder()
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        expireCount.incrementAndGet();
                    }
                })
                .withLeaseOfferExpirySecs(1000000)
                .build();
        final VirtualMachineLease lease1 = LeaseProvider.getLeaseOffer("host1", 2, 2000, 1, 10);
        scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), Collections.singletonList(lease1));
        Thread.sleep(100);
        scheduler.expireLease(lease1.getId());
        List<TaskRequest> tasks = new ArrayList<>();
        tasks.add(TaskRequestProvider.getTaskRequest(2, 1000, 1));
        final SchedulingResult result = scheduler.scheduleOnce(tasks, Collections.<VirtualMachineLease>emptyList());
        Assert.assertEquals(0, result.getResultMap().size());
    }

    // test that offers are rejected based on time irrespective of how many offers are outstanding.
    @Test
    public void testTimedOfferRejects() throws Exception {
        final AtomicInteger expireCount = new AtomicInteger();
        final int leaseExpirySecs=2;
        final TaskScheduler scheduler = new TaskScheduler.Builder()
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        expireCount.incrementAndGet();
                    }
                })
                .withLeaseOfferExpirySecs(leaseExpirySecs)
                .withRejectAllExpiredOffers()
                .build();
        final int nLeases=100;
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(nLeases, 4, 4000, 1, 10);
        for(int i=0; i<leaseExpirySecs; i++) {
            scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), leases);
            leases.clear();
            Thread.sleep(1000);
        }
        leases = LeaseProvider.getLeases(nLeases, nLeases, 4, 4000, 1, 10);
        for(int i=0; i<leaseExpirySecs-1; i++) {
            scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), leases);
            leases.clear();
            Thread.sleep(1000);
        }
        Assert.assertEquals(nLeases, expireCount.get());
        for(int i=0; i<2; i++) {
            scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), leases);
            leases.clear();
            Thread.sleep(1000);
        }
        Assert.assertEquals(nLeases*2, expireCount.get());
    }

    // test that all offers of a VM are rejected when one of them expires
    @Test
    public void testRejectAllOffersOfVm() throws Exception {
        final AtomicInteger expireCount = new AtomicInteger();
        final int leaseExpirySecs=2;
        final Set<String> hostsRejectedFrom = new HashSet<>();
        final AtomicBoolean gotReject = new AtomicBoolean();
        final TaskScheduler scheduler = new TaskScheduler.Builder()
                .withLeaseRejectAction(virtualMachineLease -> {
                    expireCount.incrementAndGet();
                    hostsRejectedFrom.add(virtualMachineLease.hostname());
                    gotReject.set(true);
                })
                .withLeaseOfferExpirySecs(leaseExpirySecs)
                .withMaxOffersToReject(1)
                .build();
        final int nhosts = 2;
        List<VirtualMachineLease> leases = LeaseProvider.getLeases(nhosts, 4, 4000, 1, 10);
        // add the same leases with same hostnames twice again, so there are 3 offers for each of the nHosts.
        leases.addAll(LeaseProvider.getLeases(nhosts, 4, 4000, 1, 10));
        leases.addAll(LeaseProvider.getLeases(nhosts, 4, 4000, 1, 10));
        for (int i=0; i<leaseExpirySecs+2 && !gotReject.get(); i++) {
            scheduler.scheduleOnce(Collections.<TaskRequest>emptyList(), leases);
            leases.clear();
            Thread.sleep(1000);
        }
        Assert.assertEquals(3, expireCount.get());
        Assert.assertEquals(1, hostsRejectedFrom.size());
    }
}
