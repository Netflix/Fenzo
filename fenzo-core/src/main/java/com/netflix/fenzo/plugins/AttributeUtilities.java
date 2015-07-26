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

package com.netflix.fenzo.plugins;

import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;

import java.util.Map;

class AttributeUtilities {

    /* package */ final static String DEFAULT_ATTRIBUTE="HOSTNAME";

    /* package */ static String getAttrValue(VirtualMachineLease lease, String hostAttributeName) {
        switch (hostAttributeName) {
            case DEFAULT_ATTRIBUTE:
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
