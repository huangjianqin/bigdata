package org.kin.kafka.multithread.protocol.configcenter;

import org.kin.kafka.multithread.domain.ConfigFetchResponse;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeat;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;

/**
 * Created by huangjianqin on 2017/9/10.
 * 用于app向注册中心获取配置
 */
public interface DiamondMasterProtocol {
    ConfigFetchResponse getAppConfig(ApplicationHost appHost);
    void heartbeat(ConfigFetcherHeartbeat heartbeat);
}
