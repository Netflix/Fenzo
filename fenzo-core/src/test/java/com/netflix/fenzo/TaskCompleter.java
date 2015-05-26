package com.netflix.fenzo;

import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskCompleter {
    private final SortedSet<RandomTaskGenerator.GeneratedTask> sortedTaskSet;
    private final BlockingQueue<RandomTaskGenerator.GeneratedTask> taskInputQ;
    private final Action1<RandomTaskGenerator.GeneratedTask> taskCompleter;
    private final long delayMillis;

    public TaskCompleter(BlockingQueue<RandomTaskGenerator.GeneratedTask> taskInputQ,
                         Action1<RandomTaskGenerator.GeneratedTask> taskCompleter, long delayMillis) {
        sortedTaskSet = new TreeSet<>();
        this.taskInputQ = taskInputQ;
        this.taskCompleter = taskCompleter;
        this.delayMillis = delayMillis;
    }

    public void start() {
        final List<RandomTaskGenerator.GeneratedTask> newTasks = new ArrayList<>();
        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            newTasks.clear();
                            taskInputQ.drainTo(newTasks);
                            if(!newTasks.isEmpty()) {
                                for(RandomTaskGenerator.GeneratedTask t: newTasks)
                                    sortedTaskSet.add(t);
                            }
                            if(sortedTaskSet.isEmpty())
                                return;
                            RandomTaskGenerator.GeneratedTask runningTask = sortedTaskSet.first();
                            long now = System.currentTimeMillis();
//                            System.out.println("                                  Looking at next task to complete: now=" +
//                                    now + ", task's completion is " + runningTask.getRunUntilMillis());
                            if(runningTask.getRunUntilMillis()<=now) {
                                Iterator<RandomTaskGenerator.GeneratedTask> iterator = sortedTaskSet.iterator();
                                while(iterator.hasNext()) {
                                    RandomTaskGenerator.GeneratedTask nextTask = iterator.next();
                                    if(nextTask.getRunUntilMillis()>now)
                                        return;
                                    taskCompleter.call(nextTask);
                                    iterator.remove();
                                }
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                },
                delayMillis, delayMillis, TimeUnit.MILLISECONDS
        );
    }
}
