package io.mantisrx.fenzo.plugins;

import io.mantisrx.fenzo.ConstraintEvaluator;
import io.mantisrx.fenzo.TaskRequest;
import io.mantisrx.fenzo.TaskTracker;
import io.mantisrx.fenzo.TaskTrackerState;
import io.mantisrx.fenzo.VirtualMachineCurrentState;
import io.mantisrx.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import rx.functions.Func1;

import java.util.Map;
import java.util.Set;

/**
 * A unique constraint evaluator to constrain tasks by a given host attribute.
 * <P>
 *     This can be used to constrain a given set of tasks (co-tasks) are assigned hosts with unique
 * value for the given host attribute. For example, given a host attribute <code>ZoneAttribute</code>,
 * the co-tasks would be placed one one task per zone. This implies that any co-tasks submitted in greater
 * number than the number of zones will not get assigned any hosts. Instead, if a load balancing strategy
 * is to be obtained, the <code>asSoftConstraint()</code> provides the conversion.
 * </P>
 * <P>
 *     When constructed without a host attribute name, this constraint evaluator uses host names as the attribute
 *     for the unique constraint.
 * </P>
 */
public class UniqueHostAttrConstraint implements ConstraintEvaluator {
    private final Func1<String, Set<String>> coTasksGetter;
    private final String hostAttributeName;
    private final String name;

    public UniqueHostAttrConstraint(Func1<String, Set<String>> coTasksGetter) {
        this(coTasksGetter, AttributeUtilities.DEFAULT_ATTRIBUTE);
    }

    public UniqueHostAttrConstraint(Func1<String, Set<String>> coTasksGetter, String hostAttributeName) {
        this.coTasksGetter = coTasksGetter;
        this.hostAttributeName = hostAttributeName;
        this.name = UniqueHostAttrConstraint.class.getName()+"-"+hostAttributeName;
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
        for(String coTask: coTasks) {
            TaskTracker.ActiveTask activeTask = taskTrackerState.getAllRunningTasks().get(coTask);
            if(activeTask==null)
                activeTask = taskTrackerState.getAllCurrentlyAssignedTasks().get(coTask);
            if(activeTask!=null) {
                String usedAttrVal = AttributeUtilities.getAttrValue(activeTask.getTotalLease(), hostAttributeName);
                if(usedAttrVal==null || usedAttrVal.isEmpty())
                    return new Result(false, hostAttributeName+" attribute unavailable on host " + activeTask.getTotalLease().hostname() +
                            " running co-task " + coTask);
                if(usedAttrVal.equals(targetHostAttrVal)) {
                    return new Result(false, hostAttributeName+" " + targetHostAttrVal + " already used for another co-task " + coTask);
                }
            }
        }
        return new Result(true, "");
    }

}
