package org.kin.hbase.core.annotation;

import java.lang.annotation.*;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface Column {
    /**
     * 不能为空
     */
    String family();

    /**
     * 可为空
     * 空时取变量名
     */
    String qualifier() default "";
}
