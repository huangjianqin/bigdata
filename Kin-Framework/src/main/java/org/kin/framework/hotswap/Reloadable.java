package org.kin.framework.hotswap;

/**
 * Created by huangjianqin on 2018/1/31.
 * 标识接口
 * <p>
 * 本热更方式侵入很强,要求开发者手动实现继承抽象类,甚至有些情况,需开发定制热更替换实例逻辑
 * 热更新的实例如果不是volatile,并不保证立即'可见',可能拿到的是旧的数据(引用)
 */
public interface Reloadable {
}