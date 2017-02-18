package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.sla.ResAllocs;

/**
 * Helper comparator/matcher functions for use in unit tests.
 */
public class FenzoMatchers {

    public static boolean hasEqualResources(ResAllocs first, ResAllocs second) {
        return first.getCores() == second.getCores()
                && first.getMemory() == second.getMemory()
                && first.getNetworkMbps() == second.getNetworkMbps()
                && first.getDisk() == second.getDisk();
    }
}
