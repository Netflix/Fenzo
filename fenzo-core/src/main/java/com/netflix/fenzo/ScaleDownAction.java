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

import java.util.Collection;

public class ScaleDownAction implements AutoScaleAction {
    private final String ruleName;
    private final Collection<String> hosts;

    ScaleDownAction(String ruleName, Collection<String> hosts) {
        this.ruleName = ruleName;
        this.hosts = hosts;
        // ToDo need to ensure those hosts' offers don't get used
    }

    @Override
    public String getRuleName() {
        return ruleName;
    }

    @Override
    public Type getType() {
        return Type.Down;
    }

    public Collection<String> getHosts() {
        return hosts;
    }
}
