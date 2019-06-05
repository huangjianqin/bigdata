package org.kin.framework.asyncdb;

import java.lang.annotation.*;

/**
 * Created by huangjianqin on 2019/3/31.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface PersistentClass {
    /**
     * 持久化类 类型
     */
    Class<? extends DBSynchronzier> type();
}
