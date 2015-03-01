package io.mantisrx.fenzo.samples;

import io.mantisrx.fenzo.ConstraintEvaluator;
import io.mantisrx.fenzo.TaskRequest;
import io.mantisrx.fenzo.VMTaskFitnessCalculator;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
        return new TaskRequest() {
            @Override
            public String getId() {
                return taskId;
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
            public double getDisk() {
                return 10;
            }

            @Override
            public int getPorts() {
                return 1;
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }
        };
    }

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
