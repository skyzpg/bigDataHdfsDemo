package cn.itcast.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //获取任务对象
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
        //第一步:读取输入文件解析成key value对
        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/wordcount"));
        TextInputFormat.addInputPath(job, new Path("file:///F:\\mapreduce\\input"));
        //第二步：设置我们的Mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置我们map阶段完成之后的输出类型
        job.setMapOutputKeyClass(Text.class);//设置k2的输出类型
        job.setMapOutputValueClass(LongWritable.class);//设置v2的输出类型

        job.setCombinerClass(MyCombiner.class);
        //第三步、第四步、第五步、第六步略
        //第七步：设置我们的reduce类
        job.setReducerClass(WordCountReducer.class);
        //设置我们reduce阶段完成后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //第八步：设置输出类型以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
       // TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/wordcount_out1"));
        TextOutputFormat.setOutputPath(job, new Path("file:///F:\\mapreduce\\output"));

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }



    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Tool tool = new JobMain();
        int run = ToolRunner.run(configuration, tool, args);
        System.exit(run);
    }
}
