package org.kin.framework.actor;

import org.kin.framework.actor.impl.AbstractActor;

/**
 * Created by huangjianqin on 2018/6/5.
 */
@FunctionalInterface
public interface Message<AA extends AbstractActor<AA>> {
    void handle(AA actor);
}
