package org.kin.framework.asyncdb;

/**
 * Created by huangjianqin on 2019/4/3.
 */
public interface AsyncDBStrategy {
    /**
     * 获取每次处理DB实体的数量
     */
    int getOprNum();

    /**
     * DB操作的尝试次数
     */
    int getTryTimes();

    /**
     * 每次处理的间隔, 毫秒
     */
    int getDuration(int size);
}
