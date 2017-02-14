package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;

import java.util.List;

/**
 * Collection of helper functions for {@link com.netflix.fenzo.sla.ResAllocs} data type.
 */
public final class ResAllocsUtil {

    public static ResAllocs addAll(List<ResAllocs> resToAdd) {
        if (resToAdd.isEmpty()) {
            throw new IllegalArgumentException("Empty list provided");
        }
        if (resToAdd.size() == 1) {
            return resToAdd.get(0);
        }
        double cores = 0;
        double memory = 0;
        double network = 0;
        double disk = 0;
        for (ResAllocs r : resToAdd) {
            cores += r.getCores();
            memory += r.getMemory();
            network += r.getNetworkMbps();
            disk += r.getDisk();
        }
        return new ResAllocsBuilder(resToAdd.get(0).getTaskGroupName())
                .withCores(cores)
                .withMemory(memory)
                .withNetworkMbps(network)
                .withDisk(disk)
                .build();
    }

    public static ResAllocs subtract(ResAllocs first, ResAllocs second) {
        return new ResAllocsBuilder(first.getTaskGroupName())
                .withCores(first.getCores() - second.getCores())
                .withMemory(first.getMemory() - second.getMemory())
                .withNetworkMbps(first.getNetworkMbps() - second.getNetworkMbps())
                .withDisk(first.getDisk() - second.getDisk())
                .build();
    }

    public static ResAllocs ceilingOf(ResAllocs first, ResAllocs second) {
        return new ResAllocsBuilder(first.getTaskGroupName())
                .withCores(Math.max(first.getCores(), second.getCores()))
                .withMemory(Math.max(first.getMemory(), second.getMemory()))
                .withNetworkMbps(Math.max(first.getNetworkMbps(), second.getNetworkMbps()))
                .withDisk(Math.max(first.getDisk(), second.getDisk()))
                .build();
    }

    public static boolean isLess(QueuableTask task, ResAllocs second) {
        if (task.getCPUs() >= second.getCores()) {
            return false;
        }
        if (task.getMemory() >= second.getMemory()) {
            return false;
        }
        if (task.getNetworkMbps() >= second.getNetworkMbps()) {
            return false;
        }
        if (task.getDisk() >= second.getDisk()) {
            return false;
        }
        return true;
    }

    public static boolean isLess(ResAllocs first, ResAllocs second) {
        if (first.getCores() >= second.getCores()) {
            return false;
        }
        if (first.getMemory() >= second.getMemory()) {
            return false;
        }
        if (first.getNetworkMbps() >= second.getNetworkMbps()) {
            return false;
        }
        if (first.getDisk() >= second.getDisk()) {
            return false;
        }
        return true;
    }

    public static ResAllocs emptyOf(String name) {
        return new ResAllocsBuilder(name).build();
    }
}
