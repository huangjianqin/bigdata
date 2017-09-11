package org.kin.kafka.multithread.protocol.configcenter;

import org.kin.kafka.multithread.protocol.app.ApplicationConfig;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/10.
 * 用于app向注册中心获取配置
 */
public interface DiamondMasterProtocol {
    Properties getAppConfig(ApplicationHost appHost);
}
