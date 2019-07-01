package org.kin.framework.actor.impl;

import org.kin.framework.actor.Receive;
import org.kin.framework.actor.domain.PoisonPill;
import org.kin.framework.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by huangjianqin on 2018/6/5.
 * <p>
 * 预定义方法匹配builder
 */
public class ReceiveBuilder {
    private static final Logger log = LoggerFactory.getLogger("actor");
    //match和matchEqual根据定义顺序排序,matchAny总是在match和matchEqual之后执行，并且也是根据定义顺序排序
    private List<AbstractFuncWrapper> funcWrappers = new ArrayList<>();

    private ReceiveBuilder() {
    }

    //-----------------------------------------------------------------------------------------------
    public static ReceiveBuilder create() {
        return new ReceiveBuilder();
    }

    public <AA extends AbstractActor<AA>, T> ReceiveBuilder match(Class<T> type, Receive.Func<AA, T> func) {
        funcWrappers.add(new TypeMatchFuncWrapper<>(type, func));
        return this;
    }

    public <AA extends AbstractActor<AA>, T> ReceiveBuilder matchEqual(T t, Receive.Func<AA, T> func) {
        funcWrappers.add(new MatchEqualFuncWrapper<>(t, func));
        return this;
    }

    public <AA extends AbstractActor<AA>> ReceiveBuilder matchAny(Receive.Func<AA, ?> func) {
        funcWrappers.add(new MatchAnyFuncWrapper(func));
        return this;
    }

    public <AA extends AbstractActor<AA>> ReceiveBuilder dead(Receive.Func<AA, PoisonPill> func) {
        return match(PoisonPill.class, func);
    }

    public Receive build() {
        Collections.sort(this.funcWrappers);
        return new InternalReceive(Collections.unmodifiableList(this.funcWrappers));
    }

    //-----------------------------------------------------------------------------------------------

    /**
     * Predicate用于匹配message
     * Comparable用于排序方法
     */
    private abstract class AbstractFuncWrapper<AA extends AbstractActor<AA>, T> implements Predicate, Comparable {
        private final Receive.Func<AA, T> func;

        protected AbstractFuncWrapper(Receive.Func<AA, T> func) {
            this.func = func;
        }

        void checkAndExecute(AA applier, Object oArg) {
            if (test(oArg)) {
                try {
                    func.apply(applier, (T) oArg);
                } catch (Exception e) {
                    ExceptionUtils.log(e);
                }
            }
        }
    }

    private class TypeMatchFuncWrapper<AA extends AbstractActor<AA>, T> extends AbstractFuncWrapper<AA, T> {
        private Class<T> type;

        private TypeMatchFuncWrapper(Class<T> type, Receive.Func<AA, T> func) {
            super(func);
            this.type = type;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public boolean test(Object o) {
            return type.isInstance(o);
        }
    }

    private class MatchEqualFuncWrapper<AA extends AbstractActor<AA>, T> extends AbstractFuncWrapper<AA, T> {
        private T t;

        protected MatchEqualFuncWrapper(T t, Receive.Func<AA, T> func) {
            super(func);
            this.t = t;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public boolean test(Object o) {
            return t.equals(o);
        }
    }

    private class MatchAnyFuncWrapper<AA extends AbstractActor<AA>, T> extends AbstractFuncWrapper<AA, T> {

        protected MatchAnyFuncWrapper(Receive.Func<AA, T> func) {
            super(func);
        }

        @Override
        public int compareTo(Object o) {
            return 1;
        }

        @Override
        public boolean test(Object o) {
            return true;
        }

    }

    //-----------------------------------------------------------------------------------------------
    private class InternalReceive implements Receive {
        private final List<AbstractFuncWrapper> funcWrappers;

        private InternalReceive(List<AbstractFuncWrapper> funcWrappers) {
            this.funcWrappers = funcWrappers;
        }


        @Override
        public <AA extends AbstractActor<AA>, T> void receive(AA applier, T message) {
            for (AbstractFuncWrapper funcWrapper : funcWrappers) {
                //所有匹配的方法都会处理该条message
                funcWrapper.checkAndExecute(applier, message);
            }
        }
    }
}
