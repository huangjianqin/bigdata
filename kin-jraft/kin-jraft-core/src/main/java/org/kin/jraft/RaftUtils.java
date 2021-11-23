package org.kin.jraft;

import org.kin.kinrpc.serialization.Serialization;
import org.kin.kinrpc.serialization.protobuf.ProtobufSerialization;

import java.nio.ByteBuffer;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public final class RaftUtils {
    /** 快照文件后缀 */
    public static final String SNAPSHOT_FILE_NAME = "data";
    /** {@link com.alipay.sofa.jraft.entity.Task#setData(ByteBuffer)} 默认使用的序列化方法 */
    public static final Serialization PROTOBUF = new ProtobufSerialization();

    private RaftUtils() {
    }
}
