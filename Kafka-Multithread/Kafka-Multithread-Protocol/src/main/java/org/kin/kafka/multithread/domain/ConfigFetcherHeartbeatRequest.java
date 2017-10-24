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
    private List<ApplicationContextInfo> succeedAppNames = new ArrayList<>();
    private List<ApplicationContextInfo> failAppNames = new ArrayList<>();
    private long requestTime;

    public ConfigFetcherHeartbeatRequest() {
    }

    public ConfigFetcherHeartbeatRequest(
            ApplicationContextInfo appHost,
            List<ApplicationContextInfo> succeedAppNames,
            List<ApplicationContextInfo> failAppNames,
            long requestTime
    ) {
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

    public List<ApplicationContextInfo> getSucceedAppNames() {
        return succeedAppNames;
    }

    public void setSucceedAppNames(List<ApplicationContextInfo> succeedAppNames) {
        this.succeedAppNames = succeedAppNames;
    }

    public List<ApplicationContextInfo> getFailAppNames() {
        return failAppNames;
    }

    public void setFailAppNames(List<ApplicationContextInfo> failAppNames) {
        this.failAppNames = failAppNames;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }
}
