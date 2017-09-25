package org.kin.kafka.multithread.domain;

/**
 * Created by huangjianqin on 2017/9/25.
 */
public class ConfigResultRequest {
    private String appName;
    private boolean isSucceed;
    private long requestTime;
    private Throwable e;

    public ConfigResultRequest() {
    }

    public ConfigResultRequest(String appName, boolean isSucceed, long requestTime, Throwable e) {
        this.appName = appName;
        this.isSucceed = isSucceed;
        this.requestTime = requestTime;
        this.e = e;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
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
