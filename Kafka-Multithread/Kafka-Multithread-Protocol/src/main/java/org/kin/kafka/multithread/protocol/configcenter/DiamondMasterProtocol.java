package org.kin.kafka.multithread.protocol.configcenter;

import org.kin.kafka.multithread.domain.ConfigFetchResult;
import org.kin.kafka.multithread.domain.ConfigSetupResult;
import org.kin.kafka.multithread.protocol.app.ApplicationConfig;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;

import java.util.List;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/10.
 * 用于app向注册中心获取配置
 */
public interface DiamondMasterProtocol {
    ConfigFetchResult getAppConfig(ApplicationHost appHost);
    void configFail(ConfigSetupResult result);
}
