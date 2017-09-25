package org.kin.kafka.multithread.domain;

import org.kin.kafka.multithread.protocol.app.ApplicationHost;

import java.util.List;

/**
 * Created by huangjianqin on 2017/9/25.
 */
public class ConfigFetcherHeartbeat {
    private ApplicationHost appHost;
    private List<String> succeedAppNames;
    private List<String> failAppNames;
    private long requestTime;

    public ConfigFetcherHeartbeat() {
    }

    public ConfigFetcherHeartbeat(ApplicationHost appHost, List<String> succeedAppNames, List<String> failAppNames, long requestTime) {
        this.appHost = appHost;
        this.succeedAppNames = succeedAppNames;
        this.failAppNames = failAppNames;
        this.requestTime = requestTime;
    }

    public ApplicationHost getAppHost() {
        return appHost;
    }

    public void setAppHost(ApplicationHost appHost) {
        this.appHost = appHost;
    }

    public List<String> getSucceedAppNames() {
        return succeedAppNames;
    }

    public void setSucceedAppNames(List<String> succeedAppNames) {
        this.succeedAppNames = succeedAppNames;
    }

    public List<String> getFailAppNames() {
        return failAppNames;
    }

    public void setFailAppNames(List<String> failAppNames) {
        this.failAppNames = failAppNames;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }
}
