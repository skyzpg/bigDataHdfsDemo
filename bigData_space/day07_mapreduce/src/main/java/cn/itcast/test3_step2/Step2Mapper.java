package cn.itcast.test3_step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Step2Mapper extends Mapper<LongWritable,Text,Text,Text> {
    /*      k1          v1
    *       0       A-F-C-J-E-	B
            15      E-F-	    M
            21      J-I-H-A-F-	O
    * -------------------------------
    *       k2           v2
    *       A-C          B
    *       A-E          B
    *       A-F          B
    */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        String v2 = split[1];
        String[] strs = split[0].split("-");

        Arrays.sort(strs);//对数组做排序

        for(int i =0;i<strs.length-1;i++){

            for(int j=i+1;j<strs.length;j++){

                context.write(new Text(strs[i]+"-"+strs[j]), new Text(v2));
            }
        }
    }
}
