package org.kin.framework.event;

/**
 * Created by 健勤 on 2017/8/8.
 * 事件接口
 */
public interface Event<TYPE extends Enum<TYPE>> {
    TYPE getType();

    long getTimestamp();
    @Override
    String toString();
}
