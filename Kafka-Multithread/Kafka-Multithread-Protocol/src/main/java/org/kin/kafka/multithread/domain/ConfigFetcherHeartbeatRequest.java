package org.kin.kafka.multithread.domain;

import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangjianqin on 2017/9/25.
 */
public class ConfigFetcherHeartbeatRequest implements Serializable {
    private ApplicationContextInfo appHost;
    private List<String> succeedAppNames = new ArrayList<>();
    private List<String> failAppNames = new ArrayList<>();
    private long requestTime;

    public ConfigFetcherHeartbeatRequest() {
    }

    public ConfigFetcherHeartbeatRequest(ApplicationContextInfo appHost, List<String> succeedAppNames, List<String> failAppNames, long requestTime) {
        this.appHost = appHost;
        this.succeedAppNames = succeedAppNames;
        this.failAppNames = failAppNames;
        this.requestTime = requestTime;
    }

    public ApplicationContextInfo getAppHost() {
        return appHost;
    }

    public void setAppHost(ApplicationContextInfo appHost) {
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
