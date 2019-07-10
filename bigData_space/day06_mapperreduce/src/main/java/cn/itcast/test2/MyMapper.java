package cn.itcast.test2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取手机号 作为k2
        String[] strs = value.toString().split("\t");
        String phoneNum = strs[1];

        //获取四个流量字段
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(strs[6]));
        flowBean.setDownFlow(Integer.parseInt(strs[7]));
        flowBean.setUpCountFlow(Integer.parseInt(strs[8]));
        flowBean.setDownCountFlow(Integer.parseInt(strs[9]));
        //将k2.v2写人context
        context.write(new Text(phoneNum),flowBean );

    }
}
