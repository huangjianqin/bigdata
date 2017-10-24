package org.kin.kafka.multithread.domain;

import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;

import java.io.Serializable;

/**
 * Created by huangjianqin on 2017/9/25.
 */
public class ConfigResultRequest implements Serializable{
    private ApplicationContextInfo applicationContextInfo;
    private boolean isSucceed;
    private long requestTime;
    private Throwable e;

    public ConfigResultRequest() {
    }

    public ConfigResultRequest(ApplicationContextInfo applicationContextInfo, boolean isSucceed, long requestTime, Throwable e) {
        this.applicationContextInfo = applicationContextInfo;
        this.isSucceed = isSucceed;
        this.requestTime = requestTime;
        this.e = e;
    }

    public ApplicationContextInfo getApplicationContextInfo() {
        return applicationContextInfo;
    }

    public void setApplicationContextInfo(ApplicationContextInfo applicationContextInfo) {
        this.applicationContextInfo = applicationContextInfo;
    }


    public boolean isSucceed() {
        return isSucceed;
    }

    public void setSucceed(boolean succeed) {
        isSucceed = succeed;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    public Throwable getE() {
        return e;
    }

    public void setE(Throwable e) {
        this.e = e;
    }
}
