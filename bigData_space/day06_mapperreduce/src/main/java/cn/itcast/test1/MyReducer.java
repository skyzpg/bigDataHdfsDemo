package cn.itcast.test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<PairWritable,IntWritable,Text,IntWritable> {

    private Text outPutKey = new Text();
    @Override
    protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        for (IntWritable value : values) {
            outPutKey.set(key.getWord());
            context.write(outPutKey,value );
        }
    }
}
