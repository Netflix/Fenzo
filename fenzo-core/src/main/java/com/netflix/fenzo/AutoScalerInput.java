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

import java.util.List;
import java.util.Set;

class AutoScalerInput {
    private final List<VirtualMachineLease> idleResourcesList;
    private final List<VirtualMachineLease> idleInactiveResources;
    private final Set<TaskRequest> failedTasks;

    AutoScalerInput(List<VirtualMachineLease> idleResources, List<VirtualMachineLease> idleInactiveResources, Set<TaskRequest> failedTasks) {
        this.idleResourcesList= idleResources;
        this.idleInactiveResources = idleInactiveResources;
        this.failedTasks = failedTasks;
    }

    public List<VirtualMachineLease> getIdleResourcesList() {
        return idleResourcesList;
    }

    public List<VirtualMachineLease> getIdleInactiveResourceList() {
        return idleInactiveResources;
    }

    public Set<TaskRequest> getFailures() {
        return failedTasks;
    }
}
