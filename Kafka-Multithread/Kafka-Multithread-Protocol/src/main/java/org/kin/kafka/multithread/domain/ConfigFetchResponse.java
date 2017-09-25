package org.kin.kafka.multithread.domain;

import java.util.List;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class ConfigFetchResponse {
    private List<Properties> newConfigs;
    private long fetchTime;

    public ConfigFetchResponse() {
    }

    public ConfigFetchResponse(List<Properties> newConfigs, long fetchTime) {
        this.newConfigs = newConfigs;
        this.fetchTime = fetchTime;
    }

    public List<Properties> getNewConfigs() {
        return newConfigs;
    }

    public long getFetchTime() {
        return fetchTime;
    }
}
