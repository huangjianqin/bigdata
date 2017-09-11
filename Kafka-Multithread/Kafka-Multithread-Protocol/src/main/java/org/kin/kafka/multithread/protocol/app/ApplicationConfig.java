package org.kin.kafka.multithread.protocol.app;

import java.io.Serializable;

/**
 * Created by huangjianqin on 2017/9/10.
 */
public class ApplicationConfig implements Serializable {
    private String config;
    private String type;

    public ApplicationConfig() {
    }

    public ApplicationConfig(String config, String type) {
        this.config = config;
        this.type = type;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
