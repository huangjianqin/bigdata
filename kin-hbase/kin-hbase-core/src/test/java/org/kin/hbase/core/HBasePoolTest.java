package org.kin.hbase.core;

import org.kin.hbase.core.op.HBaseOps;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBasePoolTest extends HBasePoolTestBase {
    public static void main(String[] args) {
        HEntity entity = new HEntity("r2", "v3", "v4");

        HBaseOps.put("test").put(entity);

        HEntity h1 = HBaseOps.get("test", "r1").family("t1").one(HEntity.class);
        System.out.println(h1);
        System.out.println("----------------------------------------------------------------------");
        for (HEntity he : HBaseOps.scan("test", "r1", "r2").includeStop("r2").batch(HEntity.class)) {
            System.out.println(he);
        }
    }
}
