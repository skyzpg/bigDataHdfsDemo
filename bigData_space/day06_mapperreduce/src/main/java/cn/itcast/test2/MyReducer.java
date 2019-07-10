package cn.itcast.test2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.FileOutputStream;
import java.io.IOException;

public class MyReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //将v2重新封装
        FlowBean flowBean = new FlowBean();
        Integer upFlow =0;
        Integer  downFlow=0;
        Integer upCountFlow=0;
        Integer downCountFlow=0;

        for (FlowBean value : values) {
            upFlow+=value.getUpFlow();
            downFlow+=value.getDownFlow();
            upCountFlow+=value.getUpCountFlow();
            downCountFlow+=value.getDownCountFlow();
        }
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        //将k3,v3写入context
        context.write(key,flowBean );
    }
}
