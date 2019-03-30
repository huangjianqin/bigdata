package org.kin.framework.event.dispatcher;

import org.kin.framework.event.AbstractEvent;

/**
 * Created by 健勤 on 2017/8/9.
 */
public class ThirdEvent extends AbstractEvent<ThirdEventType> {
    public ThirdEvent(ThirdEventType thirdEventType) {
        super(thirdEventType);
    }
}
