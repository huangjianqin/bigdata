package org.kin.bigdata.event;

/**
 * Created by 健勤 on 2017/8/9.
 */
public class SecondEvent extends AbstractEvent<SecondEventType> {
    public SecondEvent(SecondEventType secondEventType) {
        super(secondEventType);
    }
}
