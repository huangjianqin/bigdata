package org.kin.kafka.multithread.configcenter.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 * 本地配置
 */
public class ConfigCenterConfig {
    public static final Properties DEFAULT_CONFIG = new Properties();
    public static final Map<String, String> CONFIG2FORMATOR = new HashMap<>();

    public static final String CONFIG_STOREMANAGER_CLASS = "configstoremanager.class";
    public static final String CONFIG_STOREMANAGER_SERVER_HOST = "configstoremanager.server.host";
    public static final String CONFIG_STOREMANAGER_SERVER_PORT = "configstoremanager.server.port";
    public static final String DIAMONDMASTERPROTOCOL_PORT = "diamondmasterprotocol.port";
    public static final String ADMINPROTOCOL_PORT = "adminprotocol.port";


    static {
        DEFAULT_CONFIG.put(CONFIG_STOREMANAGER_CLASS, DefaultConfigCenterConfig.DEFAULT_CONFIG_STOREMANAGER_CLASS);
        DEFAULT_CONFIG.put(CONFIG_STOREMANAGER_SERVER_HOST, DefaultConfigCenterConfig.DEFAULT_CONFIG_STOREMANAGER_SERVER_HOST);
        DEFAULT_CONFIG.put(CONFIG_STOREMANAGER_SERVER_PORT, DefaultConfigCenterConfig.DEFAULT_CONFIG_STOREMANAGER_SERVER_PORT);
        DEFAULT_CONFIG.put(DIAMONDMASTERPROTOCOL_PORT, DefaultConfigCenterConfig.DEFAULT_DIAMONDMASTERPROTOCOL_PORT);
        DEFAULT_CONFIG.put(ADMINPROTOCOL_PORT, DefaultConfigCenterConfig.DEFAULT_ADMINPROTOCOL_PORT);
    }

    static {
        CONFIG2FORMATOR.put(CONFIG_STOREMANAGER_CLASS, ".*");
        CONFIG2FORMATOR.put(CONFIG_STOREMANAGER_SERVER_HOST, "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        CONFIG2FORMATOR.put(CONFIG_STOREMANAGER_SERVER_PORT, "\\d{1,5}");
        CONFIG2FORMATOR.put(DIAMONDMASTERPROTOCOL_PORT, "\\d{1,5}");
        CONFIG2FORMATOR.put(ADMINPROTOCOL_PORT, "\\d{1,5}");
    }
}
