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

import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Action2;
import com.netflix.fenzo.functions.Func1;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestLongRunningScheduler {

    public static interface Strategy {
        VMTaskFitnessCalculator getFitnessCalculator();
        public List<RandomTaskGenerator.TaskType> getJobTypes();
        double getFitnessGoodEnoughValue();
        Action1<Map<String, Map<VMResource, Double[]>>> getPrintAction();
        Action2<String, RandomTaskGenerator.GeneratedTask> getTaskAssigner();
        Action2<String, RandomTaskGenerator.GeneratedTask> getTaskUnAssigner();
    }

    private static final double NUM_CORES_PER_HOST = 8.0;
    private static final int NUM_HOSTS = 2000;
    private final RandomTaskGenerator taskGenerator;
    private final List<VirtualMachineLease> leases;
    private final TaskScheduler taskScheduler;
    private final BlockingQueue<RandomTaskGenerator.GeneratedTask> taskQueue;
    private final BlockingQueue<RandomTaskGenerator.GeneratedTask> taskCompleterQ = new LinkedBlockingQueue<>();
    private final Map<String, RandomTaskGenerator.GeneratedTask> allTasksMap = new HashMap<>();
    private static PrintStream outputStream;
    private static final long delayMillis=50;
    private final Strategy strategy;

    public TestLongRunningScheduler(RandomTaskGenerator taskGenerator, List<VirtualMachineLease> leases,
                                    BlockingQueue<RandomTaskGenerator.GeneratedTask> taskQueue,
                                    Strategy strategy) {
        this.taskGenerator = taskGenerator;
        this.leases = leases;
        this.taskQueue = taskQueue;
        new TaskCompleter(taskCompleterQ, new Action1<RandomTaskGenerator.GeneratedTask>() {
            @Override
            public void call(RandomTaskGenerator.GeneratedTask task) {
                System.out.println("                                               Completing task " + task.getId() + " on host " + task.getHostname());
                TestLongRunningScheduler.this.taskScheduler.getTaskUnAssigner().call(task.getId(), task.getHostname());
                TestLongRunningScheduler.this.strategy.getTaskUnAssigner().call(task.getHostname(), allTasksMap.get(task.getId()));
            }
        }, delayMillis).start();
        this.strategy = strategy;
        taskScheduler = new TaskScheduler.Builder()
                .withFitnessCalculator(strategy.getFitnessCalculator())
                .withFitnessGoodEnoughFunction(new Func1<Double, Boolean>() {
                    @Override
                    public Boolean call(Double f) {
                        return f > TestLongRunningScheduler.this.strategy.getFitnessGoodEnoughValue();
                    }
                })
                .withLeaseOfferExpirySecs(1000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        System.err.println("Unexpected to reject lease on " + lease.hostname());
                    }
                })
                .build();
    }

    private void runAll() {
        final List<TaskRequest> taskRequests = new ArrayList<>();
        final Map<String, RandomTaskGenerator.GeneratedTask> pendingTasksMap = new HashMap<>();
        taskScheduler.scheduleOnce(taskRequests, leases); // Get the original leases in
        strategy.getPrintAction().call(taskScheduler.getResourceStatus());
        final List<VirtualMachineLease>[] newLeasesArray = new ArrayList[5];
        for(int i=0; i<newLeasesArray.length; i++)
            newLeasesArray[i] = new ArrayList<>();
        final AtomicInteger counter = new AtomicInteger();
        final AtomicInteger totalTasksLaunched = new AtomicInteger();
        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                int i = counter.incrementAndGet();
                List<VirtualMachineLease> newLeases = newLeasesArray[i % 5];
                //taskQueue.drainTo(taskRequests);
                taskRequests.addAll(taskGenerator.getTasks());
                for(TaskRequest t: taskRequests) {
                    pendingTasksMap.put(t.getId(), (RandomTaskGenerator.GeneratedTask) t);
                    allTasksMap.put(t.getId(), (RandomTaskGenerator.GeneratedTask) t);
                }
                taskRequests.clear();
                int tasksToAssign = pendingTasksMap.size();
                System.out.println("Calling to schedule " + tasksToAssign  + " tasks with " + newLeases.size() + " with new leases");
                SchedulingResult schedulingResult = taskScheduler.scheduleOnce(new ArrayList<TaskRequest>(pendingTasksMap.values()), newLeases);
                newLeases.clear();
                int numTasksLaunched=0;
                int numHostsAssigned=0;
                if(schedulingResult.getResultMap() != null) {
                    for(Map.Entry<String, VMAssignmentResult> entry: schedulingResult.getResultMap().entrySet()) {
                        numHostsAssigned++;
                        for(TaskAssignmentResult result: entry.getValue().getTasksAssigned()) {
                            numTasksLaunched++;
                            totalTasksLaunched.incrementAndGet();
                            ((RandomTaskGenerator.GeneratedTask)result.getRequest()).setHostname(entry.getKey());
                            taskScheduler.getTaskAssigner().call(result.getRequest(), entry.getKey());
                            taskCompleterQ.offer((RandomTaskGenerator.GeneratedTask) result.getRequest());
                            strategy.getTaskAssigner().call(entry.getKey(), (RandomTaskGenerator.GeneratedTask) result.getRequest());
                            pendingTasksMap.remove(result.getRequest().getId());
                        }
                        newLeases.add(LeaseProvider.getConsumedLease(entry.getValue()));
                    }
                }
                strategy.getPrintAction().call(taskScheduler.getResourceStatus());
                if(tasksToAssign!=numTasksLaunched)
                    System.out.println("############ tasksToAssign=" + tasksToAssign + ", launched="+numTasksLaunched);
                System.out.printf("%d tasks launched on %d hosts, %d pending\n", numTasksLaunched, numHostsAssigned, pendingTasksMap.size());
                if(pendingTasksMap.size()>0 || counter.get()>50) {
                    System.out.println("Reached pending status in " + counter.get() + " iterations, totalTasks launched=" + totalTasksLaunched.get());
                    System.exit(0);
                }
            }
        }, delayMillis, delayMillis, TimeUnit.MILLISECONDS);
    }

    private static Action1<Map<String, Map<VMResource, Double[]>>> printResourceUtilization = new Action1<Map<String, Map<VMResource, Double[]>>>() {
        @Override
        public void call(Map<String, Map<VMResource, Double[]>> resourceStatus) {
            if(resourceStatus==null)
                return;
            int empty=0;
            int partial=0;
            int full=0;
            int totalUsed=0;
            for(Map.Entry<String, Map<VMResource, Double[]>> entry: resourceStatus.entrySet()) {
                Map<VMResource, Double[]> value = entry.getValue();
                for(Map.Entry<VMResource, Double[]> resEntry: value.entrySet()) {
                    switch (resEntry.getKey()) {
                        case CPU:
                            Double available = resEntry.getValue()[1];
                            Double used = resEntry.getValue()[0];
                            totalUsed += used;
                            if(available == NUM_CORES_PER_HOST)
                                empty++;
                            else if(used == NUM_CORES_PER_HOST)
                                full++;
                            else
                                partial++;
                    }
                }
            }
            outputStream.printf("%5.2f, %d, %d,%d\n",
                    (totalUsed * 100.0) / (double) (NUM_CORES_PER_HOST * NUM_HOSTS), empty, partial, full);
            System.out.printf("Utilization=%5.2f%% (%d totalUsed), empty=%d, partial=%d,ful=%d\n",
                    (totalUsed * 100.0) / (double) (NUM_CORES_PER_HOST * NUM_HOSTS), totalUsed, empty, partial, full);
        }
    };

    static Strategy binPackingStrategy = new Strategy() {
        @Override
        public VMTaskFitnessCalculator getFitnessCalculator() {
            return BinPackingFitnessCalculators.cpuBinPacker;
        }
        @Override
        public double getFitnessGoodEnoughValue() {
            return 1.0;
        }
        @Override
        public Action1<Map<String, Map<VMResource, Double[]>>> getPrintAction() {
            return printResourceUtilization;
        }
        @Override
        public List<RandomTaskGenerator.TaskType> getJobTypes() {
            List<RandomTaskGenerator.TaskType> taskTypes = new ArrayList<>();
            taskTypes.add(new RandomTaskGenerator.TaskType(1, 0.75, 20000, 20000));
            taskTypes.add(new RandomTaskGenerator.TaskType(3, 0.25, 10000, 10000));
            taskTypes.add(new RandomTaskGenerator.TaskType(6, 0.25, 20000, 20000));
            return taskTypes;
        }
        @Override
        public Action2<String, RandomTaskGenerator.GeneratedTask> getTaskAssigner() {
            return new Action2<String, RandomTaskGenerator.GeneratedTask>() {
                @Override
                public void call(String s, RandomTaskGenerator.GeneratedTask s2) {
                }
            };
        }
        @Override
        public Action2<String, RandomTaskGenerator.GeneratedTask> getTaskUnAssigner() {
            return new Action2<String, RandomTaskGenerator.GeneratedTask>() {
                @Override
                public void call(String s, RandomTaskGenerator.GeneratedTask s2) {
                }
            };
        }
    };

    static Strategy noPackingStrategy = new Strategy() {
        @Override
        public VMTaskFitnessCalculator getFitnessCalculator() {
            return new DefaultFitnessCalculator();
        }
        @Override
        public double getFitnessGoodEnoughValue() {
            return 1.0;
        }
        @Override
        public Action1<Map<String, Map<VMResource, Double[]>>> getPrintAction() {
            return printResourceUtilization;
        }
        @Override
        public List<RandomTaskGenerator.TaskType> getJobTypes() {
            List<RandomTaskGenerator.TaskType> taskTypes = new ArrayList<>();
            taskTypes.add(new RandomTaskGenerator.TaskType(1, 0.75, 20000, 20000));
            taskTypes.add(new RandomTaskGenerator.TaskType(3, 0.25, 10000, 10000));
            taskTypes.add(new RandomTaskGenerator.TaskType(6, 0.25, 20000, 20000));
            return taskTypes;
        }
        @Override
        public Action2<String, RandomTaskGenerator.GeneratedTask> getTaskAssigner() {
            return new Action2<String, RandomTaskGenerator.GeneratedTask>() {
                @Override
                public void call(String s, RandomTaskGenerator.GeneratedTask s2) {
                }
            };
        }
        @Override
        public Action2<String, RandomTaskGenerator.GeneratedTask> getTaskUnAssigner() {
            return new Action2<String, RandomTaskGenerator.GeneratedTask>() {
                @Override
                public void call(String s, RandomTaskGenerator.GeneratedTask s2) {
                }
            };
        }
    };

    static Strategy taskRuntimeStrategy = new Strategy() {
        private Map<String, Set<RandomTaskGenerator.GeneratedTask>> hostToTasksMap = new HashMap<>();
        public VMTaskFitnessCalculator getFitnessCalculatorNOT() {
            return new DefaultFitnessCalculator();
        }
        @Override
        public VMTaskFitnessCalculator getFitnessCalculator() {
            return new VMTaskFitnessCalculator() {
                @Override
                public String getName() {
                    return "Task Runtime Fitness Calculator";
                }
                @Override
                public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                    RandomTaskGenerator.GeneratedTask generatedTask = (RandomTaskGenerator.GeneratedTask)taskRequest;
                    int sameCount=0;
                    int total=0;
                    for(TaskRequest request: targetVM.getRunningTasks()) {
                        total++;
                        if(isSame((RandomTaskGenerator.GeneratedTask)request, generatedTask))
                            sameCount++;
                    }
                    for(TaskAssignmentResult result: targetVM.getTasksCurrentlyAssigned()) {
                        total++;
                        if(isSame((RandomTaskGenerator.GeneratedTask) result.getRequest(), generatedTask))
                            sameCount++;
                    }
                    if(total==0)
                        return 1.0;
                    return (double)sameCount/(double)total;
                }
            };
        }
        private boolean isSame(RandomTaskGenerator.GeneratedTask request, RandomTaskGenerator.GeneratedTask generatedTask) {
            if(request.getRuntimeMillis() == generatedTask.getRuntimeMillis())
                return true;
            return false;
        }
        private boolean isSame(Set<RandomTaskGenerator.GeneratedTask> requests) {
            if(requests==null || requests.isEmpty())
                return true;
            RandomTaskGenerator.GeneratedTask generatedTask = requests.iterator().next();
            for(RandomTaskGenerator.GeneratedTask task: requests)
                if(!isSame(task, generatedTask))
                    return false;
            return true;
        }
        @Override
        public List<RandomTaskGenerator.TaskType> getJobTypes() {
            List<RandomTaskGenerator.TaskType> taskTypes = new ArrayList<>();
            taskTypes.add(new RandomTaskGenerator.TaskType(1, 0.5, 10000, 10000));
            taskTypes.add(new RandomTaskGenerator.TaskType(1, 0.5, 60000, 60000));
            return taskTypes;
        }
        @Override
        public double getFitnessGoodEnoughValue() {
            return 1.0;
        }
        @Override
        public Action1<Map<String, Map<VMResource, Double[]>>> getPrintAction() {
            return new Action1<Map<String, Map<VMResource, Double[]>>>() {
                @Override
                public void call(Map<String, Map<VMResource, Double[]>> stringMapMap) {
                    int same=0;
                    int diff=0;
                    int unused=0;
                    for(String hostname: stringMapMap.keySet()) {
                        Set<RandomTaskGenerator.GeneratedTask> generatedTasks = hostToTasksMap.get(hostname);
                        if(generatedTasks==null)
                            unused++;
                        else if(isSame(generatedTasks))
                            same++;
                        else
                            diff++;
                    }
                    outputStream.printf("%d, %d, %d\n", unused, same, diff);
                    System.out.printf("Unused=%d, same=%d, different=%d\n", unused, same, diff);
                }
            };
        }
        @Override
        public Action2<String, RandomTaskGenerator.GeneratedTask> getTaskAssigner() {
            return new Action2<String, RandomTaskGenerator.GeneratedTask>() {
                @Override
                public void call(String hostname, RandomTaskGenerator.GeneratedTask task) {
                    if(hostToTasksMap.get(hostname)==null)
                        hostToTasksMap.put(hostname, new HashSet<RandomTaskGenerator.GeneratedTask>());
                    hostToTasksMap.get(hostname).add(task);
                }
            };
        }
        @Override
        public Action2<String, RandomTaskGenerator.GeneratedTask> getTaskUnAssigner() {
            return new Action2<String, RandomTaskGenerator.GeneratedTask>() {
                @Override
                public void call(String hostname, RandomTaskGenerator.GeneratedTask task) {
                    hostToTasksMap.get(hostname).remove(task);
                }
            };
        }
    };

    public static void main(String[] args) {
        try {
            final BlockingQueue<RandomTaskGenerator.GeneratedTask> taskQueue = new LinkedBlockingQueue<>();
            outputStream = new PrintStream("/tmp/test.out");
            //Strategy strategy = noPackingStrategy;
            Strategy strategy = taskRuntimeStrategy;
            VMTaskFitnessCalculator fitnessCalculator = BinPackingFitnessCalculators.cpuBinPacker;
            final double fitnessGoodEnoughValue=1.0;
            RandomTaskGenerator taskGenerator = new RandomTaskGenerator(taskQueue, delayMillis, 10, strategy.getJobTypes());
            //taskGenerator.start();
            TestLongRunningScheduler longRunningScheduler = null;

            longRunningScheduler = new TestLongRunningScheduler(
                    taskGenerator,
                    LeaseProvider.getLeases(NUM_HOSTS, NUM_CORES_PER_HOST, NUM_CORES_PER_HOST * 1000, 1, 1000),
                    taskQueue, strategy
            );
            longRunningScheduler.runAll();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
