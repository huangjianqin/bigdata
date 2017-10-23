package org.kin.kafka.multithread.distributed.node.config;

import org.kin.kafka.multithread.config.DefaultAppConfig;

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
    //最好是container.healthreport.internal的倍数
    public static final String CONTAINER_IDLETIMEOUT = "container.idletiemout";
    public static final String CONTAINER_HEALTHREPORT_INTERNAL = "container.healthreport.internal";
    public static final String CONTAINER_ALLOCATE_RETRY = "container.allocate.retry";

    //config fetcher
    public static final String CONFIGFETCHER_HEARTBEAT = "configfetcher.heartbeat";

    //配置中心节点信息
    public static final String CONFIGCENTER_HOST = "configcenter.host";
    public static final String CONFIGCENTER_PORT = "configcenter.port";

    static {
        DEFAULT_NODECONFIG.put(NODE_PROTOCOL_PORT, DefaultNodeConfig.DEFAULT_NODE_PROTOCOL_PORT);
        DEFAULT_NODECONFIG.put(CONTAINER_PROTOCOL_INITPORT, DefaultNodeConfig.DEFAULT_CONTAINER_PROTOCOL_INITPORT);
        DEFAULT_NODECONFIG.put(CONTAINER_IDLETIMEOUT, DefaultNodeConfig.DEFAULT_CONTAINER_IDLETIMEOUT);
        DEFAULT_NODECONFIG.put(CONTAINER_HEALTHREPORT_INTERNAL, DefaultNodeConfig.DEFAULT_CONTAINER_HEALTHREPORT_INTERNAL);
        DEFAULT_NODECONFIG.put(CONTAINER_ALLOCATE_RETRY, DefaultNodeConfig.DEFAULT_CONTAINER_ALLOCATE_RETRY);

        DEFAULT_NODECONFIG.put(CONFIGFETCHER_HEARTBEAT, DefaultNodeConfig.DEFAULT_CONFIGFETCHER_HEARTBEAT);

        DEFAULT_NODECONFIG.put(CONFIGCENTER_HOST, DefaultNodeConfig.DEFAULT_CONFIGCENTER_HOST);
        DEFAULT_NODECONFIG.put(CONFIGCENTER_PORT, DefaultNodeConfig.DEFAULT_CONFIGCENTER_PORT);
    }

    static {
        CONFIG2FORMATOR.put(NODE_PROTOCOL_PORT, "\\d{1,5}");
        CONFIG2FORMATOR.put(CONTAINER_PROTOCOL_INITPORT, "\\d{1,5}");
        CONFIG2FORMATOR.put(CONTAINER_IDLETIMEOUT, "\\d+");
        CONFIG2FORMATOR.put(CONTAINER_HEALTHREPORT_INTERNAL, "\\d+");
        CONFIG2FORMATOR.put(CONTAINER_ALLOCATE_RETRY, "\\d+");

        CONFIG2FORMATOR.put(CONFIGFETCHER_HEARTBEAT, "\\d+");

        CONFIG2FORMATOR.put(CONFIGCENTER_HOST, "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        CONFIG2FORMATOR.put(CONFIGCENTER_PORT, "\\d{1,5}");
    }
}
