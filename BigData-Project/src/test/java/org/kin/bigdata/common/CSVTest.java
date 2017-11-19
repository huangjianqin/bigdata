package org.kin.bigdata.common;

import au.com.bytecode.opencsv.CSVParser;

import java.io.IOException;

/**
 * Created by 健勤 on 2017/8/5.
 */
public class CSVTest {
    public static void main(String[] args) throws IOException {
        CSVParser parser = new CSVParser();
        String[] values = parser.parseLine("\"1\",2683657840,140100,\"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36 SE 2.X MetaSr 1.0\",\"Windows XP\",\"785022225.1422973265\",\"785022225.1422973265\",1422973268278,\"2015-02-03 22:21:08\",\"/info/hunyin/hunyinfagui/201404102884290_6.html\",20150203,\"http://www.lawtime.cn/info/hunyin/hunyinfagui/201404102884290_6.html\",\"107001\",\"www.lawtime.cn\",\"广东省人口与计划生育条例全文2014 - 法律快车婚姻法\",31,\"故意伤害\",\"计划生育\",NA,NA,NA,NA");
        System.out.println(values.length);
    }
}
