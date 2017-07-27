package org.bigdata.kafka.multithread.experiment;

import java.util.Properties;

/**
 * Created by hjq on 2017/6/21.
 */
public interface ReConfigable {
    void reConfig(Properties config);
}
