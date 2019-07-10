package cn.itcast.test1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //遍历values 获取v3
        String first ="";
        String second = "";
        for (Text value : values) {
            if(value.toString().startsWith("p")){
                first = value.toString();
            } else {
                second += value.toString()+"\t";
            }
        }

        String a = "aaaa";
        //将k3（productId）和v3（商品信息和订单信息）写入上下文
        context.write(key,new Text(first+"\t"+second) );
    }
}
