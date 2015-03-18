package com.netflix.fenzo.plugins;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import rx.functions.Func1;

import java.util.Map;

/**
 * A constraint that ensures that a task gets a host with an attribute of a specified value.
 */
public class HostAttrValueConstraint implements ConstraintEvaluator {
    private static final String HOSTNAME="HOSTNAME";
    private final String hostAttributeName;
    private final Func1<String, String> hostAttributeValueGetter;

    public HostAttrValueConstraint(String hostAttributeName, Func1<String, String> hostAttributeValueGetter) {
        this.hostAttributeName = hostAttributeName==null? HOSTNAME:hostAttributeName;
        this.hostAttributeValueGetter = hostAttributeValueGetter;
    }

    @Override
    public String getName() {
        return HostAttrValueConstraint.class.getName()+"-"+hostAttributeName;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        String targetHostAttrVal = getAttrValue(targetVM.getCurrAvailableResources());
        if(targetHostAttrVal==null || targetHostAttrVal.isEmpty()) {
            return new Result(false, hostAttributeName + " attribute unavailable on host " + targetVM.getCurrAvailableResources().hostname());
        }
        String requiredAttrVal = hostAttributeValueGetter.call(taskRequest.getId());
        return targetHostAttrVal.equals(requiredAttrVal)?
                new Result(true, "") :
                new Result(false, "Host attribute " + hostAttributeName + ": required=" + requiredAttrVal + ", got=" + targetHostAttrVal);
    }

    private String getAttrValue(VirtualMachineLease lease) {
        switch (hostAttributeName) {
            case HOSTNAME:
                return lease.hostname();
            default:
                Map<String,Protos.Attribute> attributeMap = lease.getAttributeMap();
                if(attributeMap==null)
                    return null;
                Protos.Attribute attribute = attributeMap.get(hostAttributeName);
                if(attribute==null)
                    return null;
                return attribute.getText().getValue();
        }
    }
}
