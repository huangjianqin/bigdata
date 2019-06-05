package org.kin.framework.event;

import java.lang.annotation.*;

/**
 * Created by huangjianqin on 2019/3/1.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface Listener {
    int MIN_ORDER = -10;
    int NORMAL_ORDER = 0;
    int MAX_ORDER = 10;

    int order() default NORMAL_ORDER;
}
