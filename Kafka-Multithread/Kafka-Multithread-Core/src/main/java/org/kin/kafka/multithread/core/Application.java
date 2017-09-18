package org.kin.kafka.multithread.core;

import org.kin.kafka.multithread.configcenter.ReConfigable;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/18.
 */
public interface Application extends ReConfigable{
    void start();
    void close();
    Properties getConfig();
}
