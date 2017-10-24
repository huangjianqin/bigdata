package org.kin.kafka.multithread.configcenter.manager;

import org.junit.Test;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ConfigCenterConfig;
import org.kin.kafka.multithread.configcenter.TestDiamondRestClient;
import org.kin.kafka.multithread.configcenter.manager.impl.RedisConfigStoreManager;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.utils.ClassUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/10/16.
 */
public class TestRedisConfigStoreManager {
    private Properties config;
    private ConfigStoreManager configStoreManager;

    public void init() throws IOException {
        //appConfig.config
        config = new Properties();
        String path = TestDiamondRestClient.class.getResource("/").getPath() + "appConfig.properties";
        config.load(new FileInputStream(new File(path)));

        //添加数据源
        config.put(ConfigCenterConfig.CONFIG_STOREMANAGER_SERVER_HOST, "127.0.0.1");
        config.put(AppConfig.APPNAME, "test1");
        config.put(ConfigCenterConfig.CONFIG_STOREMANAGER_SERVER_PORT, "6379");

        String storeManagerClass = RedisConfigStoreManager.class.getName();
        configStoreManager = (ConfigStoreManager) ClassUtils.instance(storeManagerClass);
    }

    public static String getPropertiesStr(Map properties){
        StringBuilder sb = new StringBuilder();
        for(Object key: properties.keySet()){
            sb.append(key.toString() + "=" + properties.get(key).toString() + System.lineSeparator());
        }

        return sb.toString();
    }

    public void setup() throws IOException {
        init();
        configStoreManager.setup(this.config);
    }

    public void clearup() {
        configStoreManager.clearup();
    }

    @Test
    public void storeConfig() throws IOException {
        setup();
        configStoreManager.storeConfig(this.config);
        clearup();
    }

    @Test
    public void getAppConfigMap() throws IOException {
        setup();
        String appName = config.getProperty(AppConfig.APPNAME);
        String host = config.getProperty(AppConfig.APPHOST);
        ApplicationContextInfo applicationContextInfo = new ApplicationContextInfo(appName, host);
        System.out.println(getPropertiesStr(configStoreManager.getAppConfigMap(applicationContextInfo)));
        clearup();
    }

    @Test
    public void getAllAppConfig() throws IOException {
        setup();
        String host = config.getProperty(AppConfig.APPHOST);
        ApplicationContextInfo applicationContextInfo = new ApplicationContextInfo(host);
        for(Properties properties: configStoreManager.getAllAppConfig(applicationContextInfo)){
            System.out.println(getPropertiesStr(properties));
            System.out.println("------------------------------------------------------------------------------------------------------");
        }
        clearup();
    }
}
