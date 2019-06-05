package org.kin.framework.hotswap.agent;

import java.util.List;

/**
 * Created by huangjianqin on 2019/3/1.
 */
public interface JavaAgentHotswapMBean {
    List<ClassFileInfo> getClassFileInfo();
}
