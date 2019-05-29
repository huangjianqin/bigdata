package org.kin.framework.actor.impl;

import org.kin.framework.Closeable;
import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.concurrent.SimpleThreadFactory;
import org.kin.framework.concurrent.ThreadManager;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.SysUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * Created by huangjianqin on 2018/6/5.
 */
public class ActorSystem implements Closeable{
    private static final Logger log = LoggerFactory.getLogger("actor");
    private static final Map<String, ActorSystem> name2AS = new ConcurrentHashMap<>();
    private static final String DEFAULT_AS_NAME = "default";

    static {
        ActorSystem defaultAS = new ActorSystem(DEFAULT_AS_NAME);
        name2AS.put(DEFAULT_AS_NAME, defaultAS);
    }

    private final String name;
    //该actor system下的actor
    private Map<String, AbstractActor> path2Actors = new ConcurrentHashMap<>();
    //每个actor system绑定一个线程池，并且该actor system下的actor使用该线程池
    private ThreadManager threadManager;

    private ActorSystem(String name) {
        this.name = name;
        if (name.toLowerCase().equals(DEFAULT_AS_NAME) && name2AS.containsKey(DEFAULT_AS_NAME)) {
            throw new IllegalStateException("actor system named '" + name + "' has exists!!!");
        }
        this.threadManager = new ThreadManager(
                Executors.newCachedThreadPool(new SimpleThreadFactory("actor-system-executor" + name)),
                Executors.newScheduledThreadPool(SysUtils.getSuitableThreadNum(), new SimpleThreadFactory("actor-system-schedule" + name)));

        monitorJVMClose();
    }

    private ActorSystem(String name, ThreadManager threadManager) {
        this(name);
        this.threadManager = threadManager;
    }

    public static ActorSystem create() {
        return name2AS.get(DEFAULT_AS_NAME);
    }

    public static ActorSystem create(String name) {
        return create(name);
    }

    public static ActorSystem create(String name, ThreadManager threadManager) {
        ActorSystem actorSystem = new ActorSystem(name, threadManager);
        name2AS.put(name, actorSystem);
        return actorSystem;
    }

    public static ActorSystem getActorSystem(String name) {
        return name2AS.get(name);
    }

    public <AA extends AbstractActor<AA>> AA actorOf(Class<AA> claxx, String name) {
        ActorPath actorPath = ActorPath.as(name, this);
        if (!path2Actors.containsKey(actorPath.getPath())) {
            try {
                Constructor<AA> constructor = claxx.getConstructor(ActorPath.class, ActorSystem.class);
                AA actor = constructor.newInstance(actorPath, this);
                add(actorPath, actor);
                return actor;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                ExceptionUtils.log(e);
                return null;
            }
        } else {
            return (AA) path2Actors.get(actorPath.getPath());
        }
    }

    public <AA extends AbstractActor<AA>> AA actorOf(ActorPath actorPath) {
        return (AA) path2Actors.get(actorPath.getPath());
    }

    public <AA extends AbstractActor<AA>> AA actorOf(String name) {
        ActorPath actorPath = ActorPath.as(name, this);
        return (AA) path2Actors.get(actorPath.getPath());
    }

    public void add(ActorPath actorPath, AbstractActor aa) {
        if (!path2Actors.containsKey(actorPath.getPath())) {
            path2Actors.put(actorPath.getPath(), aa);
        }
        throw new IllegalStateException("actor of '" + actorPath.getPath() + "' has exists!!!");
    }

    public void remove(ActorPath actorPath) {
        AbstractActor actor = path2Actors.remove(actorPath.getPath());
        actor.stop();
    }

    public String getRoot() {
        return name + "/";
    }

    public void shutdown() {
        name2AS.remove(name);
        for (AbstractActor actor : path2Actors.values()) {
            actor.stop();
        }
        //延迟1min关闭线程池
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                path2Actors = null;
                threadManager.shutdown();
                threadManager = null;
            }
        }, 1 * 60 * 1000);
    }

    //getter
    ThreadManager getThreadManager() {
        return threadManager;
    }

    @Override
    public void close() {
        shutdown();
    }
}
