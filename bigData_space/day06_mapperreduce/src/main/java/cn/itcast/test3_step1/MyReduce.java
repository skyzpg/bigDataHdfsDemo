package cn.itcast.test3_step1;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class MyReduce extends Reducer<FlowBean,Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //以手机号为k3,数据流量为v3
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
