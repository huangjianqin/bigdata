package org.kin.kafka.api;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/18.
 */
public interface Application {
    void start();

    void close();

    Properties getConfig();
}
