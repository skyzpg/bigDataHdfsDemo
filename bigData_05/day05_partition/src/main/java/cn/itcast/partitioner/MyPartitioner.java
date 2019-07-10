package cn.itcast.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text,NullWritable> {
    /**
     * 返回值表示我们的数据要去到哪个分区
     * 返回值只是一个分区的标记，标记所有相同的数据去到指定的分区
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //获取分类对应的数字
        String result = text.toString().split("\t")[5];
        if(Integer.parseInt(result) > 15){
            return 1;
        } else {
            return 0;
        }
    }
}
