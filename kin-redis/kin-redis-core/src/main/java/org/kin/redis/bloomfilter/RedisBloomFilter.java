package org.kin.redis.bloomfilter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.dynamic.RedisCommandFactory;
import org.kin.framework.utils.MurmurHash3;
import org.kin.framework.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 基于redis bitmap实现的bloom filter
 *
 * @author huangjianqin
 * @date 2022/3/4
 * @see com.google.common.hash.BloomFilter
 */
public final class RedisBloomFilter<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisBloomFilter.class);
    private final BatchBitCommands commands;

    /** redis Key */
    private final String key;
    /** bit数组长度 */
    private final long bitmapLength;
    /** Hash函数个数 */
    private final int hashFunctionNum;
    /** 目标对象转换成byte[]逻辑 */
    private final Function<T, byte[]> mapper;

    public RedisBloomFilter(String key, int predictElementSize, StatefulConnection<?, ?> connection) {
        //FYI, for 3%, we always get 5 hash functions
        this(key, predictElementSize, 0.03, o -> o.toString().getBytes(StandardCharsets.UTF_8), connection);
    }

    public RedisBloomFilter(String key, int predictElementSize, double fpp, StatefulConnection<?, ?> connection) {
        this(key, predictElementSize, fpp, o -> o.toString().getBytes(StandardCharsets.UTF_8), connection);
    }

    /**
     * @param key                redis键
     * @param predictElementSize 预估数据量
     * @param fpp                误判率
     */
    public RedisBloomFilter(String key, int predictElementSize, double fpp, Function<T, byte[]> mapper, StatefulConnection<?, ?> connection) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "redis key must not blank");
        Preconditions.checkArgument(predictElementSize > 0, "predictElementSize must be greater than 0");
        Preconditions.checkArgument(fpp > 0, "fpp must be greater than 0");

        this.key = key;
        //计算bit数组长度
        bitmapLength = (int) (-predictElementSize * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        //计算hash函数个数
        hashFunctionNum = Math.max(1, (int) Math.round((double) bitmapLength / predictElementSize * Math.log(2)));
        this.mapper = mapper;

        RedisCommandFactory factory = new RedisCommandFactory(connection, Arrays.asList(ByteArrayCodec.INSTANCE, ByteArrayCodec.INSTANCE));
        commands = factory.getCommands(BatchBitCommands.class);
    }

    /**
     * 插入对象
     *
     * @param object 目标对象
     */
    public void put(T object) {
        Preconditions.checkNotNull(object);

        for (long index : getBitIndices(object)) {
            commands.setbit(key, index, 1);
        }
        commands.flush();
    }

    /**
     * 检查元素在集合中是否存在(基于bloom filter的特性, 可能误判)
     *
     * @param object 目标对象
     */
    public boolean contains(T object) {
        try {
            return containsAsync(object).toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    /**
     * 检查元素在集合中是否存在(基于bloom filter的特性, 可能误判), 如果超时则返回false
     *
     * @param object    目标对象
     * @param timeoutMs 等待超时时间
     */
    public boolean contains(T object, int timeoutMs) {
        try {
            return containsAsync(object).toCompletableFuture().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return false;
        }
    }

    /**
     * 检查元素在集合中是否存在(基于bloom filter的特性, 可能误判)
     *
     * @param object 目标对象
     */
    public CompletionStage<Boolean> containsAsync(T object) {
        Preconditions.checkNotNull(object);

        List<RedisFuture<Long>> futures = new ArrayList<>();
        CompletionStage<Boolean> ret = null;
        for (long index : getBitIndices(object)) {
            CompletionStage<Boolean> stage = commands.getbit(key, index).thenApply(r -> r == 1);
            if (Objects.isNull(ret)) {
                ret = stage;
            } else {
                ret = ret.thenCombine(stage, (r1, r2) -> r1 && r2);
            }
        }
        commands.flush();
        return ret;
    }

    /**
     * 计算目标对象哈希后映射到Bitmap的哪些index上
     * 参考{@link com.google.common.hash.BloomFilterStrategies#MURMUR128_MITZ_64}的put(T,Funnel,int,LockFreeBitArray)
     *
     * @param object 元素值
     * @return bit下标的数组
     */
    private long[] getBitIndices(T object) {
        byte[] bytes = mapper.apply(object);
        long[] longs = MurmurHash3.hash128(bytes);
        long hash1 = longs[0];
        long hash2 = longs[1];

        long[] indeces = new long[hashFunctionNum];
        //起点hash1
        long combinedHash = hash1;
        for (int i = 0; i < hashFunctionNum; i++) {
            //保证combinedHash为正数并且在[0, bitmapLength)范围内
            indeces[i] = (combinedHash & Long.MAX_VALUE) % bitmapLength;
            //递增hash2
            combinedHash += hash2;
        }

        System.out.println(Arrays.toString(indeces));
        return indeces;
    }

    //getter
    public String getKey() {
        return key;
    }

    public long getBitmapLength() {
        return bitmapLength;
    }

    public int getHashFunctionNum() {
        return hashFunctionNum;
    }
}
