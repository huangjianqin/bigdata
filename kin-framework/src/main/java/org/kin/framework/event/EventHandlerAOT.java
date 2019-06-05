package org.kin.framework.event;

import java.lang.annotation.*;

/**
 * Created by huangjianqin on 2019/3/30.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface EventHandlerAOT {
}
