package cn.itcast.test1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if(fileName.equals("product.txt")){
            //读取的是商品表，将商品表的id作为k2，商品信息作为v2
            String[] strs = value.toString().split(",");
            String productId = strs[0];
            context.write(new Text(productId), value);
        } else{
            //读取的是订单表 将商品表的id作为k2，订单信息作为v2
            String[] strs = value.toString().split(",");
            String productId = strs[2];
            context.write(new Text(productId), value);
        }
    }
}
