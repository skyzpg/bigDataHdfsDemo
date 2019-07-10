package cn.itcast.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
*将k1.v1转化为k2.v2
*/
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString(); //读取文件 获取每行内容
        String[] split = line.split(","); //将内容按照“，”切割成单词
        //循环遍历每行达到的单词  将k1,v1转化成k2,v2  (k2为每个单词，v2为固定值：1，)
        //模型为：<hello>  <1>
        for (String word : split) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
