package org.kin.framework.hotswap.old;

import java.nio.file.Path;
import java.util.List;

/**
 * Created by huangjianqin on 2018/2/2.
 */
public abstract class AbstractHotswapFactory {
    //最新的classloader
    protected DynamicClassLoader parent;

    /**
     * @param changedPath 文件有变动的路径
     */
    public abstract void reload(List<Path> changedPath);
}
