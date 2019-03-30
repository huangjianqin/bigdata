package org.kin.framework.event;

/**
 * Created by huangjianqin on 2019/3/1.
 */
public class SimpleListenerManagerTest {
    public static void main(String[] args) {
        Object bean = new Listener3Impl();
        System.out.println(getOrder(Listener1.class, bean));
    }

    private static int getOrder(Class<?> key, Object o) {
        Class claxx = o.getClass();
        while (claxx != null) {
            if (claxx.isAnnotationPresent(Listener.class)) {
                //子类有注解, 直接使用子类注解的order
                return ((Listener) claxx.getAnnotation(Listener.class)).order();
            }
            //继续从父类寻找@Listener注解
            claxx = claxx.getSuperclass();
        }

        //取key接口的order
        return ((Listener) key.getAnnotation(Listener.class)).order();
    }
}

@Listener
interface Listener1 {
    void call();
}

@Listener
interface Listener2 {
    void call2();
}

class Listener1Impl implements Listener1 {

    @Override
    public void call() {
        System.out.println("Listener1Impl");
    }
}

@Listener(order = Listener.MAX_ORDER)
class Listener2Impl implements Listener1 {

    @Override
    public void call() {
        System.out.println("Listener2Impl");
    }
}

class Listener3Impl extends Listener2Impl implements Listener2 {

    @Override
    public void call() {
        System.out.println("Listener2Impl");
    }

    @Override
    public void call2() {
        System.out.println("Listener2Impl-call2");
    }
}