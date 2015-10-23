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
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
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
}
