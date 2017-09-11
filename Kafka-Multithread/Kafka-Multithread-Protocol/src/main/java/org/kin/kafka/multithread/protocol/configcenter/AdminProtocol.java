package org.kin.kafka.multithread.protocol.configcenter;

import org.kin.kafka.multithread.protocol.app.ApplicationConfig;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;

import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/10.
 * Admin动态管理所有app配置
 */
public interface AdminProtocol {
    Map<String, Object> storeConfig(String appName, String host, String type, String config);
    Map<String, Object> getAppConfigStr(String appName, String host, String type);
}
