package cn.itcast.test3_step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Mapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //拆分value
        String[] split = value.toString().split(":");
        String v2 = split[0];
        String[] strs = split[1].split(",");
        for (String str : strs) {
            context.write(new Text(str),new Text(v2) );
        }
    }
}
