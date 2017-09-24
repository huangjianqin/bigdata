package org.kin.kafka.multithread.distributed.node.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class NodeConfig {
    public static final Properties DEFAULT_NODECONFIG = new Properties();
    public static final Map<String, String> CONFIG2FORMATOR = new HashMap<>();


    public static final String NODE_PROTOCOL_PORT = "node.protocol.port";
    public static final String CONTAINER_PROTOCOL_INITPORT = "container.protocol.initport";
    public static final String CONTAINER_IDLETIMEOUT = "container.idletiemout";
    public static final String CONTAINER_HEALTHREPORT_INTERNAL = "container.healthreport.internal";

    static {
        DEFAULT_NODECONFIG.put(NODE_PROTOCOL_PORT, DefaultNodeConfig.DEFAULT_NODE_PROTOCOL_PORT);
        DEFAULT_NODECONFIG.put(CONTAINER_PROTOCOL_INITPORT, DefaultNodeConfig.DEFAULT_CONTAINER_PROTOCOL_INITPORT);
        DEFAULT_NODECONFIG.put(CONTAINER_IDLETIMEOUT, DefaultNodeConfig.DEFAULT_CONTAINER_IDLETIMEOUT);
        DEFAULT_NODECONFIG.put(CONTAINER_HEALTHREPORT_INTERNAL, DefaultNodeConfig.DEFAULT_CONTAINER_HEALTHREPORT_INTERNAL);
    }

    static {
        CONFIG2FORMATOR.put(NODE_PROTOCOL_PORT, "\\d*");
        CONFIG2FORMATOR.put(CONTAINER_PROTOCOL_INITPORT, "\\d*");
        CONFIG2FORMATOR.put(CONTAINER_IDLETIMEOUT, "\\d*");
        CONFIG2FORMATOR.put(CONTAINER_HEALTHREPORT_INTERNAL, "\\d*");
    }
}
