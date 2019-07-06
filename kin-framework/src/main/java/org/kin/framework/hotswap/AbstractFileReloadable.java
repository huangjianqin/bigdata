package org.kin.framework.hotswap;

import java.io.InputStream;

/**
 * Created by huangjianqin on 2018/2/1.
 * 热更新文件，特别是配置文件
 */
public abstract class AbstractFileReloadable implements Reloadable {
    private final String filePath;

    public AbstractFileReloadable(String filePath) {
        this.filePath = filePath;

        FileMonitor.instance().monitorFile(filePath, this);
    }

    public String getFilePath() {
        return filePath;
    }

    protected abstract void reload(InputStream is);
}
