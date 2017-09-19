package org.kin.kafka.multithread.protocol.distributed;

import java.util.List;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/18.
 * node向对应的container push配置信息
 *
 * 为什么不用pull?
 * 在当前场景中,配置往往不是经常变化,
 * 所以如果使用Container定时向node pull配置信息,插入队列,按(插入)顺序更新配置,
 * 这样子消耗太多没必要的系统资源,
 * 所以采用Node的push方式
 */
public interface ContainerMasterProtocol {
    Boolean updateConfig(List<Properties> configs);
}
