package org.kin.kafka.multithread.protocol.app;

import java.io.Serializable;

/**
 * Created by huangjianqin on 2017/9/10.
 */
public class ApplicationHost implements Serializable{
    private String appName;
    private String host;

    public ApplicationHost() {
    }

    public ApplicationHost(String appName, String host) {
        this.appName = appName;
        this.host = host;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

}
