package org.kin.framework.event.dispatcher;

import org.kin.framework.event.Event;

/**
 * Created by 健勤 on 2017/8/9.
 */
public class SecondEvent implements Event<SecondEventType> {
    public SecondEvent(SecondEventType secondEventType) {

    }

    @Override
    public SecondEventType getType() {
        return SecondEventType.S;
    }

    @Override
    public long getTimestamp() {
        return 0;
    }
}
