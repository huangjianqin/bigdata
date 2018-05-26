package org.kin.framework.hotswap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.kin.framework.collection.ConcurrentHashSet;
import org.kin.framework.hotswap.agent.JavaAgentHotswapFactory;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Created by huangjianqin on 2018/2/1.
 * 文件监听器
 * 利用nio 新api监听文件变换
 * 该api底层本质上是监听了操作系统的文件系统触发的文件更改事件
 *
 * 异步热加载
 */
public class FileMonitor extends Thread {
    private static final Logger log = LoggerFactory.getLogger("FileMonitor");
    //默认实现
    private static FileMonitor monitor = new FileMonitor();
    private static boolean isStarted = false;

    private WatchService watchService;
    private Set<String> monitorPaths;
    //hash(file name) -> Reloadable 实例
    private Multimap<Integer, Reloadable> monitorItems;
    //类热加载工厂
    private HotswapFactory hotswapFactory = new CommonHotswapFactory();
    //分段锁
    private Object[] locks;
    //执行线程
    private ExecutorService executorService = Executors.newFixedThreadPool(
            5,
            r -> {
                Thread thread = new Thread(r);
                thread.setName("file-monitor-reload-thread");
                return thread;
            }
    );
    private boolean isStopped = false;

    public FileMonitor() {
    }

    public FileMonitor(HotswapFactory hotswapFactory) {
        this.hotswapFactory = hotswapFactory;
    }

    public static FileMonitor instance() {
        if(!isStarted){
            monitor.start();
        }
        return monitor;
    }

    private void init(){
        try {
            watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }

        monitorPaths = new ConcurrentHashSet<>();
        monitorItems = ArrayListMultimap.create();
        locks = new Object[5];
        for(int i = 0; i < locks.length; i++){
            locks[i] = new Object();
        }
        if(hotswapFactory instanceof CommonHotswapFactory || hotswapFactory instanceof JavaAgentHotswapFactory){
            monitorClasspath();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown();
        }));
    }

    /**
     * 对于CommonHotswapFactory / JavaAgentHotswapFactory,监听整个classpath
     * 因为无法感知具体实例是啥
     */
    private void monitorClasspath(){
        String classRoot = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        Path classRootPath = Paths.get(classRoot);
        try {
            Stream<Path> allDir = Files.walk(classRootPath).filter(p -> Files.isDirectory(p));
            allDir.forEach(p -> {
                try {
                    p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                } catch (IOException e) {
                    ExceptionUtils.log(e);
                }
            });
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }
    }

    @Override
    public synchronized void start() {
        init();
        super.start();
    }

    @Override
    public void run() {
        log.info("file monitor start");
        while(!isStopped && !Thread.currentThread().isInterrupted()){
            List<Path> changedClasses = new ArrayList<>();
            try {
                WatchKey key = watchService.take();
                Path parentPath = (Path) key.watchable();
                List<WatchEvent<?>> events = key.pollEvents();
                events.forEach(event -> {
                    String itemName = event.context().toString();
                    int hashKey = itemName.hashCode();
                    Path childPath = Paths.get(parentPath.toString(), itemName);
                    log.debug("'{}' changed", childPath.toString());
                    if (!Files.isDirectory(childPath) && monitorItems.containsKey(hashKey)) {
                        if(itemName.endsWith(ClassUtils.CLASS_SUFFIX)){
                            changedClasses.add(childPath);
                        }
                        else{
                            synchronized (monitorItems){
                                Collection<Reloadable> reloadables = monitorItems.get(hashKey);
                                if(reloadables.size() > 0){
                                    executorService.execute(() -> {
                                        try {
                                            try(InputStream is = new FileInputStream(childPath.toFile())){
                                                for(Reloadable reloadable: reloadables){
                                                    if(reloadable instanceof FileReloadable){
                                                        FileReloadable fileReloadable = (FileReloadable) reloadable;
                                                        fileReloadable.reload(is);
                                                        //重置指针
                                                        is.reset();
                                                    }
                                                }
                                            }
                                        } catch (IOException e) {
                                            ExceptionUtils.log(e);
                                        }
                                    });
                                }
                            }
                        }
                    }
                });
                if(key.reset()){
                    //重置状态，让key等待事件
                    break;
                }
            } catch (InterruptedException e) {
                ExceptionUtils.log(e);
            }

            if(changedClasses.size() > 0){
                executorService.execute(() -> hotswapFactory.reload(changedClasses));
            }
        }
        log.info("file monitor end");
    }

    /**
     * 获取分段锁
     */
    private Object getLock(int key){
        return locks[key % locks.length];
    }

    public void shutdown(){
        checkStatus();

        isStopped = true;
        try {
            watchService.close();
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }
        executorService.shutdown();
        //中断监控线程
        interrupt();

        if(this == monitor){
            //如果是中断默认的文件monitor
            monitor = new FileMonitor();
            isStarted = false;
        }
    }

    private void checkStatus(){
        if(isStopped){
            throw new IllegalStateException("file monitor has shutdowned");
        }
    }

    /**
     * 监听class
     */
    public void monitorClass(Class<?> claxx){
        checkStatus();
        URL res = claxx.getResource("");
        Path dir;
        try {
            dir = Paths.get(res.toURI());
            String itemName = claxx.getSimpleName() + ClassUtils.CLASS_SUFFIX;
            monitorFile0(dir, itemName, null);
        } catch (URISyntaxException e) {
            ExceptionUtils.log(e);
        }
    }

    public void monitorFile(String pathStr, Reloadable reloadable){
        checkStatus();
        Path path = Paths.get(pathStr);
        monitorFile(path, reloadable);
    }

    public void monitorFile(Path path, Reloadable reloadable){
        checkStatus();
        monitorFile0(path.getParent(), path.getFileName().toString(), reloadable);
    }

    private void monitorFile0(Path dir, String itemName, Reloadable reloadable){
        boolean exists = monitorPaths.add(dir.toString());
        if(!exists){
            try {
                dir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            } catch (IOException e) {
                ExceptionUtils.log(e);
            }
        }

        int key = itemName.hashCode();
        synchronized (getLock(key)){
            monitorItems.put(key, reloadable);
        }
    }
}
