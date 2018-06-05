package org.kin.framework.actor.impl;

import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.concurrent.ThreadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2018/6/5.
 */
public class ActorSystem {
    private static final Logger log = LoggerFactory.getLogger("Actor");
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
    private ThreadManager threadManager = ThreadManager.DEFAULT;


    private Future future;

    private ActorSystem(String name) {
        this.name = name;
        if(name.toLowerCase().equals(DEFAULT_AS_NAME) && name2AS.containsKey(DEFAULT_AS_NAME)){
            throw new RuntimeException("can not override default actor system");
        }
    }

    private ActorSystem(String name, ThreadManager threadManager) {
        this(name);
        this.threadManager = threadManager;
    }

    public static ActorSystem create(){
        return name2AS.get(DEFAULT_AS_NAME);
    }

    public static ActorSystem create(String name){
        ActorSystem actorSystem = new ActorSystem(name);
        name2AS.put(name, actorSystem);
        return actorSystem;
    }

    public static ActorSystem create(String name, ThreadManager threadManager){
        ActorSystem actorSystem = new ActorSystem(name, threadManager);
        name2AS.put(name, actorSystem);
        return actorSystem;
    }

    public <AA extends AbstractActor<AA>> AA actorOf(Class<AA> claxx, String name){
        ActorPath actorPath = ActorPath.as(name, this);
        if(!path2Actors.containsKey(actorPath.getPath())){
            try {
                Constructor<AA> constructor = claxx.getConstructor(ActorPath.class, ActorSystem.class);
                return constructor.newInstance(actorPath, this);
            } catch (NoSuchMethodException e) {
                log.error("", e);
            } catch (IllegalAccessException e) {
                log.error("", e);
            } catch (InstantiationException e) {
                log.error("", e);
            } catch (InvocationTargetException e) {
                log.error("", e);
            }
        }

        throw new RuntimeException("actor of '" + actorPath.getPath() + "' has existed");
    }

    public void add(ActorPath actorPath, AbstractActor aa){
        if(future != null){
            boolean cancelResult = future.cancel(false);
            if(!cancelResult){
                //取消失败, actor system按流程关闭
                return;
            }
            else{
                future = null;
            }
        }
        path2Actors.put(actorPath.getPath(), aa);
    }

    public void remove(ActorPath actorPath){
        path2Actors.remove(actorPath.getPath());
        if(path2Actors.size() <= 0 && future == null){
            future = threadManager.schedule(() -> {
                if (path2Actors.size() <= 0) {
                    shutdown();
                }
            }, 5, TimeUnit.SECONDS);
        }
    }

    public <AA extends AbstractActor<AA>> AA get(ActorPath actorPath){
        return (AA) path2Actors.get(actorPath.getPath());
    }

    public String getRoot(){
        return name + "/";
    }

    public void shutdown(){
        path2Actors = null;
        threadManager.shutdown();
        threadManager = null;

        name2AS.remove(name);
    }

    //getter
    public ThreadManager getThreadManager() {
        return threadManager;
    }
}
