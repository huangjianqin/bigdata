package org.kin.hbase.core.test;

import org.kin.hbase.core.op.HBaseOps;

/**
 * Created by huangjianqin on 2018/5/27.
 */
public class Test extends TestBase{
    public static void main(String[] args) {
        HEntity entity = new HEntity("r2", "v3", "v4");

        HBaseOps.put("test").put(entity);

        HEntity h1 = HBaseOps.get("test", "r1").family("t1").one(HEntity.class);
        System.out.println(h1);
        System.out.println("----------------------------------------------------------------------");
        for(HEntity he: HBaseOps.scan("test", "r1", "r2").includeStop("r2").batch(HEntity.class)){
            System.out.println(he);
        }
    }
}
