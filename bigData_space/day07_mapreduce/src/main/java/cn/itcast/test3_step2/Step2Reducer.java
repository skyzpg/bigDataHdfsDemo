package cn.itcast.test3_step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step2Reducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {

            buffer.append(value.toString()).append("-");
        }

        String s = buffer.toString();

        if(s.endsWith("-")){
            String substring = s.substring(0, s.length() - 1);
            context.write(new Text(key.toString()+":"), new Text(substring));
        }
    }
}
