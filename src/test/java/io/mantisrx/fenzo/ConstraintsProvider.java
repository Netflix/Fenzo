package io.mantisrx.fenzo;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import org.apache.mesos.Protos;

import java.util.Map;

public class ConstraintsProvider {

    public static ConstraintEvaluator getHostAttributeHardConstraint(final String attributeName, final String value) {
        return new ConstraintEvaluator() {
            @Override
            public String getName() {
                return "HostAttributeHardConstraint";
            }
            @Override
            public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                final Map<String, Protos.Attribute> attributeMap = targetVM.getCurrAvailableResources().getAttributeMap();
                if(attributeMap==null || attributeMap.isEmpty())
                    return new Result(false, "No Attributes for host " + targetVM.getCurrAvailableResources().hostname());
                final Protos.Attribute val = attributeMap.get(attributeName);
                if(val==null)
                    return new Result(false, "No " + attributeName + " attribute for host " + targetVM.getCurrAvailableResources().hostname());
                if(val.hasText()) {
                    if(val.getText().getValue().equals(value))
                        return new Result(true, "");
                    else
                        return new Result(false, "asking for " + value + ", have " + val.getText().getValue() + " on host " +
                                targetVM.getCurrAvailableResources().hostname());
                }
                return new Result(false, "Attribute has no value");
            }
        };
    }

}
