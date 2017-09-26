/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.fenzo.queues.tiered;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class QueuableTaskProvider {

    public static QueuableTask wrapTask(final QAttributes qAttributes, final TaskRequest taskRequest) {
        final AtomicLong readyAt = new AtomicLong(0L);
        return new QueuableTask() {
            @Override
            public QAttributes getQAttributes() {
                return qAttributes;
            }

            @Override
            public String getId() {
                return taskRequest.getId();
            }

            @Override
            public String taskGroupName() {
                return taskRequest.taskGroupName();
            }

            @Override
            public double getCPUs() {
                return taskRequest.getCPUs();
            }

            @Override
            public double getMemory() {
                return taskRequest.getMemory();
            }

            @Override
            public double getNetworkMbps() {
                return taskRequest.getNetworkMbps();
            }

            @Override
            public double getDisk() {
                return taskRequest.getDisk();
            }

            @Override
            public int getPorts() {
                return taskRequest.getPorts();
            }

            @Override
            public Map<String, Double> getScalarRequests() {
                return taskRequest.getScalarRequests();
            }

            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return taskRequest.getCustomNamedResources();
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return taskRequest.getHardConstraints();
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return taskRequest.getSoftConstraints();
            }

            @Override
            public void setAssignedResources(AssignedResources assignedResources) {
                taskRequest.setAssignedResources(assignedResources);
            }

            @Override
            public AssignedResources getAssignedResources() {
                return taskRequest.getAssignedResources();
            }

            @Override
            public void safeSetReadyAt(long when) {
                readyAt.set(when);
            }

            @Override
            public long getReadyAt() {
                return readyAt.get();
            }
        };
    }
}
