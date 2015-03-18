package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTracker;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BalancedHostAttrConstraint implements ConstraintEvaluator {
    private final String name;
    private final Func1<String, Set<String>> coTasksGetter;
    private final String hostAttributeName;
    private final int expectedValues;

    public BalancedHostAttrConstraint(Func1<String, Set<String>> coTasksGetter, String hostAttributeName, int expectedValues) {
        this.coTasksGetter = coTasksGetter;
        this.hostAttributeName = hostAttributeName;
        this.name = BalancedHostAttrConstraint.class.getName()+"-"+hostAttributeName;
        this.expectedValues = expectedValues;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Set<String> coTasks = coTasksGetter.call(taskRequest.getId());
        String targetHostAttrVal = AttributeUtilities.getAttrValue(targetVM.getCurrAvailableResources(), hostAttributeName);
        if(targetHostAttrVal==null || targetHostAttrVal.isEmpty()) {
            return new Result(false, hostAttributeName + " attribute unavailable on host " + targetVM.getCurrAvailableResources().hostname());
        }
        Map<String, Integer> usedAttribsMap = null;
        try {
            usedAttribsMap = getUsedAttributesMap(coTasks, taskTrackerState);
        } catch (Exception e) {
            return new Result(false, e.getMessage());
        }
        final Integer integer = usedAttribsMap.get(targetHostAttrVal);
        if(integer==null || usedAttribsMap.isEmpty())
            return new Result(true, "");
        int min=Integer.MAX_VALUE;
        int max=Integer.MIN_VALUE;
        for(Integer i: usedAttribsMap.values()) {
            min = Math.min(min, i);
            max = Math.max(max, i);
        }
        min = expectedValues>usedAttribsMap.size()? 0 : min;
        if(min == max || integer<max)
            return new Result(true, "");
        return new Result(false, "Would further imbalance by host attribute " + hostAttributeName);
    }

    private Map<String, Integer> getUsedAttributesMap(Set<String> coTasks, TaskTrackerState taskTrackerState) throws Exception {
        Map<String, Integer> usedAttribsMap = new HashMap<>();
        for(String coTask: coTasks) {
            TaskTracker.ActiveTask activeTask = taskTrackerState.getAllRunningTasks().get(coTask);
            if(activeTask==null)
                activeTask = taskTrackerState.getAllCurrentlyAssignedTasks().get(coTask);
            if(activeTask!=null) {
                String usedAttrVal = AttributeUtilities.getAttrValue(activeTask.getTotalLease(), hostAttributeName);
                if(usedAttrVal==null || usedAttrVal.isEmpty())
                    throw new Exception(hostAttributeName+" attribute unavailable on host " + activeTask.getTotalLease().hostname() +
                            " running co-task " + coTask); // indicate missing attribute in host
                if(usedAttribsMap.get(usedAttrVal)==null)
                    usedAttribsMap.put(usedAttrVal, 1);
                else
                    usedAttribsMap.put(usedAttrVal, usedAttribsMap.get(usedAttrVal)+1);
            }
        }
        return usedAttribsMap;
    }

    public VMTaskFitnessCalculator asSoftConstraint() {
        return new VMTaskFitnessCalculator() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                String targetHostAttrVal = AttributeUtilities.getAttrValue(targetVM.getCurrAvailableResources(), hostAttributeName);
                if(targetHostAttrVal==null || targetHostAttrVal.isEmpty()) {
                    return 0.0;
                }
                Set<String> coTasks = coTasksGetter.call(taskRequest.getId());
                Map<String, Integer> usedAttribsMap = null;
                try {
                    usedAttribsMap = getUsedAttributesMap(coTasks, taskTrackerState);
                } catch (Exception e) {
                    return 0.0;
                }
                final Integer integer = usedAttribsMap.get(targetHostAttrVal);
                if(integer==null)
                    return 1.0;
                if(usedAttribsMap.isEmpty())
                    return 1.0;
                double avg=0.0;
                for(Integer i: usedAttribsMap.values())
                    avg += i;
                avg = Math.ceil(avg+1 / Math.max(expectedValues, usedAttribsMap.size()));
                if(integer<=avg)
                    return (avg-(double)integer) / avg;
                return 0.0;
            }
        };
    }
}
