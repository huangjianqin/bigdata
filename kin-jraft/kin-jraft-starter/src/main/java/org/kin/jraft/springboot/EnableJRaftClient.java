package org.kin.jraft.springboot;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author huangjianqin
 * @date 2021/11/25
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(JRaftClientMarkerConfiguration.class)
public @interface EnableJRaftClient {
}
