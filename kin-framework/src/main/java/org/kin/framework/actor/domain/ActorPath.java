package org.kin.framework.actor.domain;

import org.kin.framework.actor.impl.ActorSystem;

/**
 * Created by huangjianqin on 2018/6/5.
 * <p>
 * Actor的唯一标识
 */
//TODO 自动生成url形式的path
public class ActorPath {
    //合并需全局唯一
    private String parent;
    private String name;

    //host or ip
    private String host;
    private int port;

    private ActorPath(String parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    public static ActorPath as(String name, ActorSystem actorSystem) {
        return new ActorPath(actorSystem.getRoot(), name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ActorPath actorPath = (ActorPath) o;

        return getPath().equals(actorPath.getPath());
    }

    @Override
    public int hashCode() {
        return getPath().hashCode();
    }

    //getter
    public String getPath() {
        return parent + name;
    }

    public String getParent() {
        return parent;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
