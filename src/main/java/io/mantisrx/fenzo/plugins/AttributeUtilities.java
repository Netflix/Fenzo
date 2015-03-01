package io.mantisrx.fenzo.plugins;

import io.mantisrx.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;

import java.util.Map;

public class AttributeUtilities {

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
