package org.kin.framework.event;

import java.lang.annotation.*;

/**
 * Created by huangjianqin on 2019/3/1.
 * <p>
 * 注解事件处理器 or 方法
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@Inherited
public @interface HandleEvent {
    Class<?> type();
}
