package org.kin.bigdata.event;

/**
 * Created by 健勤 on 2017/8/8.
 */
public interface EventHandler<T extends Event> {
    void handle(T event);
}
