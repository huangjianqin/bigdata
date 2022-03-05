package org.kin.redis.bloomfilter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.dynamic.Commands;
import io.lettuce.core.dynamic.batch.BatchExecutor;
import io.lettuce.core.dynamic.batch.BatchSize;

/**
 * 支持微批处理的redis setbit getbitcommand实现
 * {@link BatchSize}标识一批command的数量, 如果不手动{@link BatchExecutor#flush()}的话, 会一直缓存在client
 *
 * @author huangjianqin
 * @date 2022/3/4
 */
@BatchSize(512)
public interface BatchBitCommands extends Commands, BatchExecutor {
    RedisFuture<Long> setbit(String key, long offset, int value);

    RedisFuture<Long> getbit(String key, long offset);
}
