package cn.itcast.test2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartition extends Partitioner<Text,FlowBean> {


    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {

        String str = text.toString();
        if (str.startsWith("135")){
            return 0;
        }else if(str.startsWith("136")){
            return 1;
        }else if(str.startsWith("137")){
            return 2;
        }else{
            return 3;
        }
    }
}
