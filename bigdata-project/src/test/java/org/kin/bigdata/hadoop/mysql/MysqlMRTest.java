package org.kin.bigdata.hadoop.mysql;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by 健勤 on 2017/7/19.
 */
public class MysqlMRTest extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("mysql-hadoop-test");
        job.setJarByClass(MysqlMRTest.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        //最后的field必须是给定的, 若选择全部, 则给"*", 否则是具体的field name
        //参数分别是Job实例, DBWritable实例, table name, where Condition, order by, selected field
        DBInputFormat.setInput(job, UserWritable.class, "user_info", "id < 500", "name", "id", "name", "description");
        //参数分别是Configuration实例, driver classpath, jdbc url, user, password
        DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://ubuntu:3306/test?useUnicode=true&characterEncoding=UTF-8", "hjq", "hjq");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(UserWritable.class);
//        FileOutputFormat.setOutputPath(job, new Path("dbRestult"));
        DBOutputFormat.setOutput(job, "user_info", "id", "name", "description");
        job.setMapperClass(DBMppaer.class);
        job.setReducerClass(DBReducer.class);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : -1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MysqlMRTest(), new String[]{""});
    }
}

