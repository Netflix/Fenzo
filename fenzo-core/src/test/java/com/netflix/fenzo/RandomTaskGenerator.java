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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RandomTaskGenerator {

    public static class TaskType {
        private final double coreSize;
        private final double ratioOfTasks;
        private final long minRunDurationMillis;
        private final long maxRunDurationMillis;

        public TaskType(double coreSize, double ratioOfTasks, long minRunDurationMillis, long maxRunDurationMillis) {
            this.coreSize = coreSize;
            this.ratioOfTasks = ratioOfTasks;
            this.minRunDurationMillis = minRunDurationMillis;
            this.maxRunDurationMillis = maxRunDurationMillis;
        }
        public double getCoreSize() {
            return coreSize;
        }
        public double getRatioOfTasks() {
            return ratioOfTasks;
        }
        public long getMinRunDurationMillis() {
            return minRunDurationMillis;
        }
        public long getMaxRunDurationMillis() {
            return maxRunDurationMillis;
        }
    }

    public static class GeneratedTask implements TaskRequest, Comparable<GeneratedTask> {
        private final TaskRequest taskRequest;
        private final long runUntilMillis;
        private final long runtimeMillis;
        private String hostname;
        private AssignedResources assignedResources=null;
        public GeneratedTask(final TaskRequest taskRequest, final long runtimeMillis, final long runUntilMillis) {
            this.taskRequest = taskRequest;
            this.runUntilMillis = runUntilMillis;
            this.runtimeMillis = runtimeMillis;
        }
        public long getRunUntilMillis() {
            return runUntilMillis;
        }
        public long getRuntimeMillis() {
            return runtimeMillis;
        }
        @Override
        public String getId() {
            return taskRequest.getId();
        }

        @Override
        public String taskGroupName() {
            return "";
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
            return 0.0;
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
            return null;
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
            this.assignedResources = assignedResources;
        }
        @Override
        public AssignedResources getAssignedResources() {
            return assignedResources;
        }

        @Override
        public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
            return Collections.emptyMap();
        }
        @Override
        public int compareTo(GeneratedTask o) {
            if(o==null)
                return -1;
            return Long.compare(runUntilMillis, o.runUntilMillis);
        }
        public String getHostname() {
            return hostname;
        }
        public void setHostname(String hostname) {
            this.hostname = hostname;
        }
    }

    private final List<TaskType> taskTypes;
    private final double memoryPerCore=1000.0;
    private final int minTasksPerCoreSizePerBatch;
    private final long interBatchIntervalMillis;
    private double minRatio;
    private final BlockingQueue<GeneratedTask> taskQueue;

    RandomTaskGenerator(BlockingQueue<GeneratedTask> taskQueue, long interBatchIntervalMillis, int minTasksPerCoreSizePerBatch, List<TaskType> taskTypes) {
        if(taskTypes==null || taskTypes.isEmpty())
            throw new IllegalArgumentException();
        this.taskQueue = taskQueue;
        this.interBatchIntervalMillis = interBatchIntervalMillis;
        this.minTasksPerCoreSizePerBatch = minTasksPerCoreSizePerBatch;
        minRatio=Double.MAX_VALUE;
        this.taskTypes = taskTypes;
        for(TaskType j: taskTypes)
            if(j.getRatioOfTasks()<minRatio)
                minRatio = j.getRatioOfTasks();
    }

    void start() {
        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                int numTasksGenerated=0;
                for(TaskType taskType: taskTypes) {
                    int numTasks = (int) (taskType.getRatioOfTasks()*minTasksPerCoreSizePerBatch/minRatio);
                    for(int t=0; t<numTasks; t++) {
                        long runtime = getRuntime(taskType.minRunDurationMillis, taskType.maxRunDurationMillis);
                        taskQueue.offer(new GeneratedTask(
                                TaskRequestProvider.getTaskRequest(taskType.coreSize, taskType.coreSize*memoryPerCore, 1),
                                runtime, now+runtime
                        ));
                        numTasksGenerated++;
                    }
                }
                System.out.println("        " + numTasksGenerated + " tasks generated");
            }
        }, interBatchIntervalMillis, interBatchIntervalMillis, TimeUnit.MILLISECONDS);
    }

    List<GeneratedTask> getTasks() {
        List<GeneratedTask> tasks = new ArrayList<>();
        long now = System.currentTimeMillis();
        for(TaskType taskType: taskTypes) {
            int numTasks = (int) (taskType.getRatioOfTasks()*minTasksPerCoreSizePerBatch/minRatio);
            for(int t=0; t<numTasks; t++) {
                long runtime = getRuntime(taskType.minRunDurationMillis, taskType.maxRunDurationMillis);
                tasks.add(new GeneratedTask(
                        TaskRequestProvider.getTaskRequest(taskType.coreSize, taskType.coreSize*memoryPerCore, 1),
                        runtime, now+runtime
                ));
            }
        }
        return tasks;
    }

    private long getRuntime(long minRunDurationMillis, long maxRunDurationMillis) {
        return minRunDurationMillis + (long)(Math.random() * (double)(maxRunDurationMillis-minRunDurationMillis));
    }
}
