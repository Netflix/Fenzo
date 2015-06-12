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

package com.netflix.fenzo.sla;

import com.netflix.fenzo.AssignmentFailure;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTracker;
import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.sla.Reservation;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ReservationEvaluator {
    private final Map<String, Reservation> reservations;
    private final TaskTracker taskTracker;
    private final Set<String> failedTaskGroups = new HashSet<>();
    private final BlockingQueue<Reservation> addQ = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> remQ = new LinkedBlockingQueue<>();
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final List<Reservation> addList = new LinkedList<>();
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final List<String> remList = new LinkedList<>();

    public ReservationEvaluator(TaskTracker taskTracker, Map<String, Reservation> initialReservations) {
        this.taskTracker = taskTracker;
        this.reservations = initialReservations==null? new HashMap<String, Reservation>() : new HashMap<>(initialReservations);
    }

    public void replaceReservation(Reservation r) {
        if(r != null)
            addQ.offer(r);
    }

    public void remReservation(String groupName) {
        if(groupName != null && !groupName.isEmpty())
            remQ.offer(groupName);
    }

    public boolean prepare() {
        failedTaskGroups.clear();
        updateReservations();
        return !reservations.isEmpty();
    }

    private void updateReservations() {
        addQ.drainTo(addList);
        if(!addList.isEmpty()) {
            Iterator<Reservation> iter = addList.iterator();
            while(iter.hasNext()) {
                final Reservation r = iter.next();
                reservations.put(r.getTaskGroupName(), r);
                iter.remove();
            }
        }
        remQ.drainTo(remList);
        if(!remList.isEmpty()) {
            Iterator<String> iter = remList.iterator();
            while(iter.hasNext()) {
                reservations.remove(iter.next());
                iter.remove();
            }
        }
    }

    public boolean taskGroupFailed(String taskGroupName) {
        return failedTaskGroups.contains(taskGroupName);
    }

    public AssignmentFailure hasReservations(TaskRequest task) {
        if(reservations.isEmpty())
            return null;
        if(failedTaskGroups.contains(task.taskGroupName()))
            return new AssignmentFailure(VMResource.Reservation, 1.0, 0.0, 0.0);
        final TaskTracker.TaskGroupUsage usage = taskTracker.getUsage(task.taskGroupName());
        final Reservation reservation = reservations.get(task.taskGroupName());
        if(reservation == null)
            return new AssignmentFailure(VMResource.Reservation, 1.0, 0.0, 0.0);
        if(usage==null) {
            final boolean success = hasZeroUsageAllowance(reservation);
            if(!success)
                failedTaskGroups.add(task.taskGroupName());
            return success? null : new AssignmentFailure(VMResource.Reservation, 1.0, 0.0, 0.0);
        }
        // currently, no way to indicate which of reservation's resources limited us
        if((usage.getCores()+task.getCPUs()) > reservation.getCores())
            return new AssignmentFailure(VMResource.Reservation, task.getCPUs(), usage.getCores(), reservation.getCores());
        if((usage.getMemory() + task.getMemory()) > reservation.getMemory())
            return new AssignmentFailure(VMResource.Reservation, task.getMemory(), usage.getMemory(), reservation.getMemory());
        if((usage.getNetworkMbps()+task.getNetworkMbps()) > reservation.getNetworkMbps())
            return new AssignmentFailure(VMResource.Reservation, task.getNetworkMbps(), usage.getNetworkMbps(), reservation.getNetworkMbps());
        if((usage.getDisk()+task.getDisk()) > reservation.getDisk())
            return new AssignmentFailure(VMResource.Reservation, task.getDisk(), usage.getDisk(), reservation.getDisk());
        return null;
    }

    private boolean hasZeroUsageAllowance(Reservation reservation) {
        return reservation != null &&
                (reservation.getCores() > 0.0 || reservation.getMemory() > 0.0 ||
                        reservation.getNetworkMbps() > 0.0 || reservation.getDisk() > 0.0);
    }

    public Map<String, Reservation> getReservations() {
        return Collections.unmodifiableMap(reservations);
    }

}
