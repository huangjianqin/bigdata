package org.kin.kafka.multithread.configcenter.manager;

import org.kin.kafka.multithread.protocol.app.ApplicationConfig;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;
import org.kin.kafka.multithread.protocol.configcenter.AdminProtocol;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 * 存储系统中统一使用key-value的properties形式进行存储,主要是为了做到格式无关,但是要保证配置写入原子性
 * 而admin可以上传或下载配置,期间就涉及格式转换
 */
public interface ConfigStoreManager{
    //appHost:appName
    String KEY_FORMAT = "%s:%s";

    void setup(Properties config);
    void clearup();
    boolean storeConfig(Properties appConfig);
    Map<String, String> getAppConfigMap(ApplicationHost appHost);
    List<Properties> getAllAppConfig(ApplicationHost appHost);
}
