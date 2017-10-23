package org.kin.kafka.multithread.domain;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class ConfigFetcherHeartbeatResponse implements Serializable {
    private List<Properties> newConfigs;
    private long responseTime;

    public ConfigFetcherHeartbeatResponse() {
    }

    public ConfigFetcherHeartbeatResponse(List<Properties> newConfigs, long responseTime) {
        this.newConfigs = newConfigs;
        this.responseTime = responseTime;
    }

    public List<Properties> getNewConfigs() {
        return newConfigs;
    }

    public long getResponseTime() {
        return responseTime;
    }
}
