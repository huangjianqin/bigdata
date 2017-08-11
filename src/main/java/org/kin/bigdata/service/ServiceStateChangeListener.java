package org.kin.bigdata.service;

/**
 * Created by 健勤 on 2017/8/8.
 */
public interface ServiceStateChangeListener {
    void onStateChanged(Service service, Service.State pre);
}
