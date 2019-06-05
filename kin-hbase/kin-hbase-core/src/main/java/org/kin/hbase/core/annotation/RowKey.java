package org.kin.hbase.core.annotation;

import java.lang.annotation.*;

/**
 * Created by huangjianqin on 2018/5/24.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface RowKey {
}
