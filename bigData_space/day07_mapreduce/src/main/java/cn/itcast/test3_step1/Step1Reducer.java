package cn.itcast.test3_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step1Reducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //String k3 ="";
        StringBuffer stringBuffer = new StringBuffer();

        for (Text value : values) {
            stringBuffer.append(value.toString()).append("-");
        }

        context.write(new Text(stringBuffer.toString()), key);
    }
}
