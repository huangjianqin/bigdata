package org.kin.kafka.multithread.configcenter.protocol;

import java.util.Properties;

/**
 * Created by hjq on 2017/6/21.
 * 用于更新kafka consumer消费配置
 */
public interface ReConfigable {
    void reConfig(Properties config);
}
