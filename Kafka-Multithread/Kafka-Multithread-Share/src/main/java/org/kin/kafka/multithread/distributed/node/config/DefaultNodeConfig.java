package org.kin.kafka.multithread.distributed.node.config;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class DefaultNodeConfig {
    public static final String DEFAULT_NODE_PROTOCOL_PORT = "60100";
    public static final String DEFAULT_CONTAINER_PROTOCOL_INITPORT = "60101";
    public static final String DEFAULT_CONTAINER_IDLETIMEOUT = Long.MAX_VALUE + "";
    public static final String DEFAULT_CONTAINER_HEALTHREPORT_INTERNAL = "1000";
    public static final String DEFAULT_CONTAINER_ALLOCATE_RETRY = "3";

    //config fetcher
    public static final String DEFAULT_CONFIGFETCHER_HEARTBEAT = 3 * 1000 + "";

    //本地
    //配置中心节点信息
    public static final String DEFAULT_CONFIGCENTER_HOST = "localhost";
    public static final String DEFAULT_CONFIGCENTER_PORT = "60001";
}
