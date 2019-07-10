package cn.itcast.test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.io.PrintStream;

public class MyMapper extends Mapper<LongWritable,Text,PairWritable,IntWritable> {

    private PairWritable mapOutKey = new PairWritable();
    private IntWritable mapOutValue = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s = value.toString();
        String[] strs = s.split("\t");
        mapOutKey.set(strs[0],Integer.valueOf(strs[1]) );
        mapOutValue.set(Integer.valueOf(strs[1]));
        context.write(mapOutKey,mapOutValue );
    }
}
