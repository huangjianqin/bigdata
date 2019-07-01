package org.kin.bigdata.hadoop.airlineclientdiscovery;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kin.framework.utils.StringUtils;

import java.io.IOException;
import java.util.Calendar;

/**
 * Created by huangjianqin on 2017/8/26.
 */
public class PreparationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = new CSVParser();
        String[] values = parser.parseLine(value.toString());

        if (filter(values)) {
            String load_time = values[9];
            String ffp_data = values[1];
            String last_to_end = values[22];
            String flight_count = values[10];
            String seg_km_sum = values[16];
            String avg_discount = values[28];

            //会员入会时间距观察窗口结束的月数
            String L = "" + getBetweenMonth(load_time, ffp_data);
            //最近一次坐飞机离观察窗口结束的月数
            String R = last_to_end;
            //观察窗口内飞行次数
            String F = flight_count;
            //观察窗口内飞行里程
            String M = seg_km_sum;
            //观察窗口内折扣系数平均值
            String C = avg_discount;

            context.write(NullWritable.get(), new Text(String.format("%s,%s,%s,%s,%s", L, R, F, M, C)));
        }
    }

    private boolean filter(String[] values) {
        //票价为空
        if (StringUtils.isBlank(values[14]) || StringUtils.isBlank(values[15])) {
            return false;
        }
        //票价为0, 平均折扣率!=0, 总飞行公里数>0
        if (Double.valueOf(values[14]).equals(0) && Double.valueOf(values[15]).equals(0) && Double.valueOf(values[28]) != 0 && Double.valueOf(values[10]) > 0) {
            return false;
        }
        return true;
    }

    private int getBetweenMonth(String first, String second) {
        String[] firsts = first.split("/");
        String[] seconds = second.split("/");

        Calendar fCalendar = getCalendar(firsts[0], firsts[1], firsts[2]);
        Calendar sCalendar = getCalendar(seconds[0], seconds[1], seconds[2]);

        return (fCalendar.get(Calendar.MONTH) - sCalendar.get(Calendar.MONTH)) + (fCalendar.get(Calendar.YEAR) - sCalendar.get(Calendar.YEAR)) * 12;
    }

    private Calendar getCalendar(String year, String month, String dayOfMonth) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, Integer.valueOf(year));
        calendar.set(Calendar.MONTH, Integer.valueOf(month));
        calendar.set(Calendar.DAY_OF_MONTH, Integer.valueOf(dayOfMonth));
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar;
    }
}


