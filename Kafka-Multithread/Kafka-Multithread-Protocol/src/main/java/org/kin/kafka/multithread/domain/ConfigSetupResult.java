package org.kin.kafka.multithread.domain;

import java.util.List;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class ConfigSetupResult {
    private List<String> failAppName;
    private long requesttime;

    public ConfigSetupResult() {
    }

    public ConfigSetupResult(List<String> failAppName, long requesttime) {
        this.failAppName = failAppName;
        this.requesttime = requesttime;
    }

    public List<String> getFailAppName() {
        return failAppName;
    }

    public long getRequesttime() {
        return requesttime;
    }
}
