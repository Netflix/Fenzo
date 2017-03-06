package com.netflix.fenzo.sla;

import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.queues.QueuableTask;

import java.util.Map;

/**
 * Collection of helper functions for {@link com.netflix.fenzo.sla.ResAllocs} data type.
 */
public final class ResAllocsUtil {

    public static ResAllocs add(ResAllocs first, ResAllocs second) {
        return new ResAllocsBuilder(first.getTaskGroupName())
                .withCores(first.getCores() + second.getCores())
                .withMemory(first.getMemory() + second.getMemory())
                .withNetworkMbps(first.getNetworkMbps() + second.getNetworkMbps())
                .withDisk(first.getDisk() + second.getDisk())
                .build();
    }

    public static ResAllocs add(ResAllocs first, QueuableTask second) {
        return new ResAllocsBuilder(first.getTaskGroupName())
                .withCores(first.getCores() + second.getCPUs())
                .withMemory(first.getMemory() + second.getMemory())
                .withNetworkMbps(first.getNetworkMbps() + second.getNetworkMbps())
                .withDisk(first.getDisk() + second.getDisk())
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

    public static boolean isBounded(QueuableTask first, ResAllocs second) {
        if (first.getCPUs() > second.getCores()) {
            return false;
        }
        if (first.getMemory() > second.getMemory()) {
            return false;
        }
        if (first.getNetworkMbps() > second.getNetworkMbps()) {
            return false;
        }
        if (first.getDisk() > second.getDisk()) {
            return false;
        }
        return true;
    }

    public static boolean isBounded(ResAllocs first, QueuableTask second) {
        if (first.getCores() > second.getCPUs()) {
            return false;
        }
        if (first.getMemory() > second.getMemory()) {
            return false;
        }
        if (first.getNetworkMbps() > second.getNetworkMbps()) {
            return false;
        }
        if (first.getDisk() > second.getDisk()) {
            return false;
        }
        return true;
    }

    public static boolean isBounded(ResAllocs first, ResAllocs second) {
        if (first.getCores() > second.getCores()) {
            return false;
        }
        if (first.getMemory() > second.getMemory()) {
            return false;
        }
        if (first.getNetworkMbps() > second.getNetworkMbps()) {
            return false;
        }
        if (first.getDisk() > second.getDisk()) {
            return false;
        }
        return true;
    }

    public static boolean hasEqualResources(ResAllocs first, ResAllocs second) {
        return first.getCores() == second.getCores()
                && first.getMemory() == second.getMemory()
                && first.getNetworkMbps() == second.getNetworkMbps()
                && first.getDisk() == second.getDisk();
    }

    public static ResAllocs empty() {
        return emptyOf("anonymous");
    }

    public static ResAllocs emptyOf(String name) {
        return new ResAllocsBuilder(name)
                .withCores(0)
                .withMemory(0)
                .withNetworkMbps(0)
                .withDisk(0)
                .build();
    }

    public static ResAllocs rename(String newName, ResAllocs allocs) {
        return new ResAllocsBuilder(newName)
                .withCores(allocs.getCores())
                .withMemory(allocs.getMemory())
                .withNetworkMbps(allocs.getNetworkMbps())
                .withDisk(allocs.getDisk())
                .build();
    }

    public static ResAllocs toResAllocs(String name, Map<VMResource, Double> resourceMap) {
        return new ResAllocsBuilder(name)
                .withCores(resourceMap.getOrDefault(VMResource.CPU, 0.0))
                .withMemory(resourceMap.getOrDefault(VMResource.Memory, 0.0))
                .withNetworkMbps(resourceMap.getOrDefault(VMResource.Network, 0.0))
                .withDisk(resourceMap.getOrDefault(VMResource.Disk, 0.0))
                .build();
    }
}
