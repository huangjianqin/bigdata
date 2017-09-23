package org.kin.kafka.multithread.configcenter.config;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class DefaultConfigCenterConfig {
    public static final String DEFALUT_CONFIGPATH = "configcenter.yml";
    public static final String DEFAULT_CONFIG_STOREMANAGER_CLASS = "org.kin.kafka.multithread.configcenter.manager.impl.RedisConfigStoreManager";
    public static final String DEFAULT_CONFIG_STOREMANAGER_SERVER_HOST = "localhost";
    public static final String DEFAULT_CONFIG_STOREMANAGER_SERVER_PORT = "6379";
    public static final String DEFAULT_DIAMONDMASTERPROTOCOL_PORT = "60001";
    public static final String DEFAULT_ADMINPROTOCOL_PORT = "60000";
}
