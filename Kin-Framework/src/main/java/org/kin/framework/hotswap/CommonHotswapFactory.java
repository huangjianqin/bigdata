package org.kin.framework.hotswap;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangjianqin on 2018/2/2.
 */
public class CommonHotswapFactory extends HotswapFactory{
    private List<ClassReloadable> classReloadableList = new ArrayList<>();

    @Override
    public void reload(List<Path> changedPath) {
        DynamicClassLoader classLoader = new DynamicClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        for(Path path: changedPath){
            Class<?> changedClass = classLoader.loadClass(path.toFile());
            for(ClassReloadable classReloadable: classReloadableList){
                classReloadable.reload(changedClass);
            }
        }
        //保存最新的classloader
        parent = classLoader;
    }
}
