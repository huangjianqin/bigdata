package org.kin.bigdata.event;

/**
 * Created by 健勤 on 2017/8/8.
 */
public interface Dispatcher {
    EventHandler getEventHandler();
    void register(Class<? extends Enum> eventType, EventHandler handler);
    void dispatch(Event event);
}
