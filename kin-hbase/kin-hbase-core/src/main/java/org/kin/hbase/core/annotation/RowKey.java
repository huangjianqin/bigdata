package org.kin.hbase.core.annotation;

import java.lang.annotation.*;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface RowKey {
}
