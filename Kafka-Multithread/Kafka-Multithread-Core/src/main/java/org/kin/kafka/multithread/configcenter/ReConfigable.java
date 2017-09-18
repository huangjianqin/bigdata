package org.kin.kafka.multithread.configcenter;

import java.util.Properties;

/**
 * Created by hjq on 2017/6/21.
 * 用于更新kafka consumer消费配置
 */
public interface ReConfigable {
    /**
     * 对于message fetcher,设置标识, 在下一轮poll时,停止receive 消息,更新自身以及message handler manager配置
     * 对于message handler manager,更新自身状态,目前只会处理减少资源的情况(需要同步),资源增加会在dispatch调用时添加
     * @param newConfig
     */
    void reConfig(Properties newConfig);
}
