package org.kin.framework.event;

/**
 * Created by 健勤 on 2017/8/8.
 * 事件分发接口
 */
public interface Dispatcher {
    /**
     * 内部将事件进队的事件处理器
     *
     * @return
     */
    EventHandler getEventHandler();

    /**
     * 注册事件处理器
     *
     * @param eventType
     * @param handler
     */
    void register(Class<? extends Enum> eventType, EventHandler handler);

    /**
     * 根据事件类型分发给对应的事件处理器
     */
    void dispatch(Event event);
}
