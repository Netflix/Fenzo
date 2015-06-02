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
