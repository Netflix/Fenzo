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

package com.netflix.fenzo.samples;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Func1;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple task generator to use with {@link SampleFramework}.
 * This is the main class to run a sample framework.
 * Create and submit a given number of tasks for given number of iterations until all tasks complete.
 */
public class TaskGenerator implements Runnable {

    private final BlockingQueue<TaskRequest> taskQueue;
    private final int numIters;
    private final int numTasks;
    private final AtomicInteger tasksCompleted = new AtomicInteger();


    public TaskGenerator(BlockingQueue<TaskRequest> taskQueue, int numIters, int numTasks) {
        this.taskQueue = taskQueue;
        this.numIters = numIters;
        this.numTasks = numTasks;
    }

    private int launchedTasks = 0;

    @Override
    public void run() {
        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < numTasks; j++)
                taskQueue.offer(getTaskRequest(launchedTasks++));
            System.out.println("        Generated " + numTasks + " tasks so far");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
    }

    private TaskRequest getTaskRequest(final int id) {
        final String taskId = "" + id;
        final AtomicReference<TaskRequest.AssignedResources> assgndResRef = new AtomicReference<>();
        return new TaskRequest() {
            @Override
            public String getId() {
                return taskId;
            }

            @Override
            public String taskGroupName() {
                return "";
            }

            @Override
            public double getCPUs() {
                return 1.0;
            }

            @Override
            public double getMemory() {
                return 1024;
            }

            @Override
            public double getNetworkMbps() {
                return 0;
            }

            @Override
            public double getDisk() {
                return 10;
            }

            @Override
            public int getPorts() {
                return 1;
            }

            @Override
            public Map<String, Double> getScalarRequests() {
                return null;
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }

            @Override
            public void setAssignedResources(AssignedResources assignedResources) {
                assgndResRef.set(assignedResources);
            }

            @Override
            public AssignedResources getAssignedResources() {
                return assgndResRef.get();
            }

            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return Collections.emptyMap();
            }
        };
    }

    /**
     * Main method to run the task generator.
     * @param args Arguments to the program. Provide as the only argument, the mesos connection string.
     */
    public static void main(String[] args) {
        if(args.length!=1) {
            System.err.println("Must provide one argument - Mesos master location string");
            System.exit(1);
        }
        int numTasks=10;
        int numIters=5;
        BlockingQueue<TaskRequest> taskQueue = new LinkedBlockingQueue<>();
        final TaskGenerator taskGenerator = new TaskGenerator(taskQueue, numIters, numTasks);
        final SampleFramework framework = new SampleFramework(taskQueue, args[0], // mesos master location string
                new Action1<String>() {
                    @Override
                    public void call(String s) {
                        taskGenerator.tasksCompleted.incrementAndGet();
                    }
                },
                new Func1<String, String>() {
                    @Override
                    public String call(String s) {
                        return "sleep 2";
                    }
                });
        long start = System.currentTimeMillis();
        (new Thread(taskGenerator)).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                framework.runAll();
            }
        }).start();
        while(taskGenerator.tasksCompleted.get() < (numIters*numTasks)) {
            System.out.println("NUM TASKS COMPLETED: " + taskGenerator.tasksCompleted.get() + " of " + (numIters*numTasks));
            try{Thread.sleep(1000);}catch(InterruptedException ie){}
        }
        System.out.println("Took " + (System.currentTimeMillis()-start) + " mS to complete " + (numIters*numTasks) + " tasks");
        framework.shutdown();
        System.exit(0);
    }
}
