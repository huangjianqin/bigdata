package org.kin.framework.hotswap;

import java.nio.file.Path;
import java.util.List;

/**
 * Created by huangjianqin on 2018/2/2.
 */
public abstract class HotswapFactory {
    //最新的classloader
    protected DynamicClassLoader parent;

    public abstract void reload(List<Path> changedPath);
}
