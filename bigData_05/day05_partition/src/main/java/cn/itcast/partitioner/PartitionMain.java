package cn.itcast.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionMain extends Configured implements
        Tool{


    @Override
    public int run(String[] strings) throws Exception {

        //获取job对象
        Job job = Job.getInstance(super.getConf(), PartitionMain.class.getSimpleName());

        //1、设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        //设置读取文件的路径
        //TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/partitioner") );
        TextInputFormat.addInputPath(job,new Path("file:///F:\\partition\\input") );

        //2、设置mapper类和数据类型
        job.setMapperClass(MyMapper.class);//设置mapper类
        job.setMapOutputKeyClass(Text.class);//设置map阶段完成之后的k2类型
        job.setMapOutputValueClass(NullWritable.class);//设置map阶段完成之后的v2类型

        //3、指定分区类
        job.setPartitionerClass(MyPartitioner.class);

        //4、第四、第五、第六步（suffle默认）

        //7、指定reducer类和数据类型（k3.v3）
        job.setReducerClass(MyReducer.class);//设置reducer类
        job.setOutputKeyClass(Text.class);//设置k3
        job.setOutputValueClass(NullWritable.class);//设置v3

        /**
         * 设置我们的分区类，以及我们的reducetask的个数，注意reduceTask的个数一定要与我们的分区数保持一致
         */
        job.setNumReduceTasks(2);//设置reduceTask的个数

        //8、设置输出k3,v3的方式
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出文件结果的路径
        //TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/outpartition") );
        TextOutputFormat.setOutputPath(job,new Path("file:///F:\\partition\\output") );

        //9、等待任务结束
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }


    public static void main(String[] args) throws Exception {
        //启动job任务
        int run = ToolRunner.run(new Configuration(), new PartitionMain(), args);
        System.exit(run);
    }

}
