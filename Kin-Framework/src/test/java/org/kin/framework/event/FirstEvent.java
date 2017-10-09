package org.kin.framework.event;

/**
 * Created by 健勤 on 2017/8/9.
 */
public class FirstEvent extends AbstractEvent<FirstEventType> {
    public FirstEvent(FirstEventType firstEventType) {
        super(firstEventType);
    }
}
