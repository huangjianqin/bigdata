package org.kin.jraft;

/**
 * @author huangjianqin
 * @date 2021/11/7
 */
public final class DefaultRaftService implements RaftService {
    public static final DefaultRaftService INSTANCE = new DefaultRaftService();

    private DefaultRaftService() {
    }

    @Override
    public void close() {
        //do nothing
    }
}
