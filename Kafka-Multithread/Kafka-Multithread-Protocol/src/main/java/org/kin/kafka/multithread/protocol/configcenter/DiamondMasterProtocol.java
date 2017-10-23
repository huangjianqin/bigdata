package org.kin.kafka.multithread.protocol.configcenter;

import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatResponse;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatRequest;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;

/**
 * Created by huangjianqin on 2017/9/10.
 * 用于app向注册中心获取配置
 */
public interface DiamondMasterProtocol {
    ConfigFetcherHeartbeatResponse heartbeat(ConfigFetcherHeartbeatRequest heartbeat);
}
