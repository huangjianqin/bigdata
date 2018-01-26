package org.kin.framework.service;

/**
 * Created by 健勤 on 2017/8/8.
 * 服务状态改变触发的监听器
 */
@FunctionalInterface
public interface ServiceStateChangeListener<S extends Service> {
    void onStateChanged(S service, Service.State pre);
}
