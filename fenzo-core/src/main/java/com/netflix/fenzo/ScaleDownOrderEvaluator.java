/*
 * Copyright 2017 Netflix, Inc.
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

import java.util.Collection;
import java.util.List;
import java.util.Set;


/**
 * A constraint evaluator that examines if VMs can be terminated, and in which order. It groups VMs into equivalence
 * classes, and orders them according to their scale down priority. If a VM instance is missing in the returned list
 * it means that it should not be terminated.
 * The scale down order within an equivalence class is determined by {@link ScaleDownConstraintExecutor}.
 */
public interface ScaleDownOrderEvaluator {

    /**
     * Returns VMs grouped into equivalence classes ordered according to their scale down priority.
     *
     * @param candidates VMs to be terminated
     * @return VMs in scale down order
     */
    List<Set<VirtualMachineLease>> evaluate(Collection<VirtualMachineLease> candidates);
}
