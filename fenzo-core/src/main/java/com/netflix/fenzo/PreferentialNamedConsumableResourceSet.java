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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This encapsulates preferential resource sets available on a VM. Resource sets are two level resources that can
 * be assigned to tasks that specify a name to reserve for an available resource set, and the number of sub-resources
 * (second level of the two level resource) it needs.
 * <P>A {@link PreferentialNamedConsumableResourceSet} contains 1 or more resource sets,
 * {@link com.netflix.fenzo.PreferentialNamedConsumableResourceSet.PreferentialNamedConsumableResource}, each of which
 * can be assigned (or reserved to) a name requested by tasks, if currently unassigned. Each resource set contains one
 * or more count of resources available for assignment to tasks.
 * <P>A task can be assigned to one of the resource sets if it either has no tasks assigned to it, or the name assigned
 * to the resource set matches what the task being assigned is requesting. The number of sub-resources requested by task
 * is also checked. Tasks may request 0 or more sub-resources. The assignment attempts to use as few resource sets as
 * possible and returns a fitness score that helps scheduler pick between multiple VMs that can potentially fit the task.
 * The resulting assignment contains the index of the resource set assigned. The resource sets are assigned indexes
 * starting with 0.
 */
public class PreferentialNamedConsumableResourceSet {

    final static String attributeName = "ResourceSet";

    private static String getResNameVal(String name, TaskRequest request) {
        final Map<String, TaskRequest.NamedResourceSetRequest> customNamedResources = request.getCustomNamedResources();
        if(customNamedResources!=null) {
            final TaskRequest.NamedResourceSetRequest setRequest = customNamedResources.get(name);
            return setRequest==null? CustomResAbsentKey : setRequest.getResValue();
        }
        return CustomResAbsentKey;
    }

    public static class ConsumeResult {
        private final int index;
        private final String attrName;
        private final String resName;
        private final double fitness;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown=true)
        public ConsumeResult(@JsonProperty("index") int index,
                             @JsonProperty("attrName") String attrName,
                             @JsonProperty("resName") String resName,
                             @JsonProperty("fitness") double fitness) {
            this.index = index;
            this.attrName = attrName;
            this.resName = resName;
            this.fitness = fitness;
        }

        public int getIndex() {
            return index;
        }

        public String getAttrName() {
            return attrName;
        }

        public String getResName() {
            return resName;
        }

        public double getFitness() {
            return fitness;
        }
    }

    public static class PreferentialNamedConsumableResource {
        private final String hostname;
        private final int index;
        private final String attrName;
        private String resName=null;
        private final int limit;
        private final Map<String, TaskRequest.NamedResourceSetRequest> usageBy;
        private int usedSubResources=0;

        PreferentialNamedConsumableResource(String hostname, int i, String attrName, int limit) {
            this.hostname = hostname;
            this.index = i;
            this.attrName = attrName;
            this.limit = limit;
            usageBy = new HashMap<>();
        }

        public int getIndex() {
            return index;
        }

        public String getResName() {
            return resName;
        }

        public int getLimit() {
            return limit;
        }

        public Map<String, TaskRequest.NamedResourceSetRequest> getUsageBy() {
            return usageBy;
        }

        public int getUsedCount() {
            if(resName == null) {
                return -1;
            }
            return usedSubResources;
        }

        double getFitness(TaskRequest request, PreferentialNamedConsumableResourceEvaluator evaluator) {
            TaskRequest.NamedResourceSetRequest setRequest = request.getCustomNamedResources()==null
                    ? null
                    : request.getCustomNamedResources().get(attrName);

            // This particular resource type is not requested. We assign to it virtual resource name 'CustomResAbsentKey',
            // and request 0 sub-resources.
            if(setRequest == null) {
                if(resName == null) {
                    return evaluator.evaluateIdle(hostname, CustomResAbsentKey, index, 0, limit);
                }
                if(resName.equals(CustomResAbsentKey)) {
                    return evaluator.evaluate(hostname, CustomResAbsentKey, index, 0, usedSubResources, limit);
                }
                return 0.0;
            }

            double subResNeed = setRequest.getNumSubResources();

            // Resource not assigned yet to any task
            if(resName == null) {
                if(subResNeed > limit) {
                    return 0.0;
                }
                return evaluator.evaluateIdle(hostname, setRequest.getResValue(), index, subResNeed, limit);
            }

            // Resource assigned different name than requested
            if(!resName.equals(setRequest.getResValue())) {
                return 0.0;
            }
            if(usedSubResources + subResNeed > limit) {
                return 0.0;
            }
            return evaluator.evaluate(hostname, setRequest.getResValue(), index, subResNeed, usedSubResources, limit);
        }

        void consume(TaskRequest request) {
            String r = getResNameVal(attrName, request);
            consume(r, request);
        }

        void consume(String assignedResName, TaskRequest request) {
            if(usageBy.get(request.getId()) != null)
                return; // already consumed
            if(resName!=null && !resName.equals(assignedResName))
                throw new IllegalStateException(this.getClass().getName() + " already consumed by " + resName +
                        ", can't consume for " + assignedResName);
            if(resName == null) {
                resName = assignedResName;
                usageBy.clear();
            }
            final TaskRequest.NamedResourceSetRequest setRequest = request.getCustomNamedResources()==null?
                    null : request.getCustomNamedResources().get(attrName);
            double subResNeed = setRequest==null? 0.0 : setRequest.getNumSubResources();
            if(usedSubResources + subResNeed > limit)
                throw new RuntimeException(this.getClass().getName() + " already consumed for " + resName +
                        " up to the limit of " + limit);
            usageBy.put(request.getId(), setRequest);
            usedSubResources += subResNeed;
        }

        boolean release(TaskRequest request) {
            String r = getResNameVal(attrName, request);
            if(resName != null && !resName.equals(r)) {
                return false;
            }
            final TaskRequest.NamedResourceSetRequest removed = usageBy.remove(request.getId());
            if(removed == null)
                return false;
            usedSubResources -= removed.getNumSubResources();
            if(usageBy.isEmpty())
                resName = null;
            return true;
        }
    }

    public static final String CustomResAbsentKey = "CustomResAbsent";
    private final String name;
    private final List<PreferentialNamedConsumableResource> usageBy;

    public PreferentialNamedConsumableResourceSet(String hostname, String name, int val0, int val1) {
        this.name = name;
        usageBy = new ArrayList<>(val0);
        for(int i=0; i<val0; i++)
            usageBy.add(new PreferentialNamedConsumableResource(hostname, i, name, val1));
    }

    public String getName() {
        return name;
    }

    public List<PreferentialNamedConsumableResource> getUsageBy() {
        return Collections.unmodifiableList(usageBy);
    }

//    boolean hasAvailability(TaskRequest request) {
//        for(PreferentialNamedConsumableResource r: usageBy) {
//            if(r.hasAvailability(request))
//                return true;
//        }
//        return false;
//    }

    ConsumeResult consume(TaskRequest request, PreferentialNamedConsumableResourceEvaluator evaluator) {
        return consumeIntl(request, false, evaluator);
    }

    void assign(TaskRequest request) {
        final TaskRequest.AssignedResources assignedResources = request.getAssignedResources();
        if(assignedResources != null) {
            final List<ConsumeResult> consumedNamedResources = assignedResources.getConsumedNamedResources();
            if(consumedNamedResources!=null && !consumedNamedResources.isEmpty()) {
                for(PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult: consumedNamedResources) {
                    if(name.equals(consumeResult.getAttrName())) {
                        final int index = consumeResult.getIndex();
                        if(index < 0 || index > usageBy.size())
                            throw new IllegalStateException("Illegal assignment of namedResource " + name +
                                    ": has " + usageBy.size() + " resource sets, can't assign to index " + index
                            );
                        usageBy.get(index).consume(consumeResult.getResName(), request);
                    }
                }
            }
        }
    }

    // returns 0.0 for no fitness at all, or <=1.0 for fitness
    double getFitness(TaskRequest request, PreferentialNamedConsumableResourceEvaluator evaluator) {
        return consumeIntl(request, true, evaluator).fitness;
    }

    private ConsumeResult consumeIntl(TaskRequest request, boolean skipConsume, PreferentialNamedConsumableResourceEvaluator evaluator) {
        PreferentialNamedConsumableResource best = null;
        double bestFitness=0.0;
        for(PreferentialNamedConsumableResource r: usageBy) {
            double f = r.getFitness(request, evaluator);
            if(f == 0.0)
                continue;
            if(bestFitness < f) {
                best = r;
                bestFitness = f;
            }
        }
        if(!skipConsume) {
            if (best == null)
                throw new RuntimeException("Unexpected to have no availability for job " + request.getId() + " for consumable resource " + name);
            best.consume(request);
        }
        return new ConsumeResult(
                best==null? -1 : best.index,
                best==null? null : best.attrName,
                best==null? null : best.resName,
                bestFitness
        );
    }

    boolean release(TaskRequest request) {
        for(PreferentialNamedConsumableResource r: usageBy)
            if(r.release(request))
                return true;
        return false;
    }

    int getNumSubResources() {
        return usageBy.get(0).getLimit()-1;
    }

    List<Double> getUsedCounts() {
        List<Double> counts = new ArrayList<>(usageBy.size());
        for(PreferentialNamedConsumableResource r: usageBy)
            counts.add((double) r.getUsedCount());
        return counts;
    }
}
