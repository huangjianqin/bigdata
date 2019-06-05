package org.kin.framework.event;

/**
 * Created by 健勤 on 2017/8/8.
 * 事件的抽象
 */
public abstract class AbstractEvent<TYPE extends Enum<TYPE>> implements Event<TYPE> {
    private final TYPE type;
    private final long timestamp;

    public AbstractEvent(TYPE type) {
        this.type = type;
        timestamp = System.currentTimeMillis();
    }

    @Override
    public final TYPE getType() {
        return type;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "EventType:" + type;
    }
}
