package cn.itcast.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long count =0;
        for (LongWritable value : values) {
            count +=value.get();
        }
        context.write(key,new LongWritable(count)); //将新的k2,v2转化成k3,v3
    }
}
