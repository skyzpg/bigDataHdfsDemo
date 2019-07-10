package cn.itcast.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable ,Text,Text,NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            //将读取的一行的数据value作为k2,v2使用一个空对象占位
            context.write(value ,NullWritable.get());
    }
}
