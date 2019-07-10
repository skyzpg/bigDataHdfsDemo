package cn.itcast.test2;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


public class MyMapper extends Mapper<LongWritable,Text,Text,Text> {

    private HashMap<String,String> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //将分布式缓存的小表数据读取到本地map集合中
        //1、获取分布式缓存列表
        URI[] files = context.getCacheFiles();
        //2:获取指定的分布式缓存文件的文件系统(FileSystem)
        FileSystem fileSystem = FileSystem.get(files[0], context.getConfiguration());

        //3、获取指定的文件输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(files[0]));
        //4、读取文件内容，将数据存入map集合
        //4.1 将字节输入流转换为字节缓冲流
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        //4.2 读取小表的内容 以行为单位 存入map集合
        String line = null;
        while ((line = bufferedReader.readLine())!=null){
            String[] split = line.split(",");
            map.put(split[0],line);
        }
        //5、关闭流
        bufferedReader.close();
        fileSystem.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //对大表的处理  实现大表和小表的join
        String[] split = value.toString().split(",");
        String productId = split[2];//得到k2
        //获取map集合中的line 与value拼接   写入上下文
        String productValue = map.get(productId);
        context.write(new Text(productId),new Text(productValue + "\t" + value.toString()) );
    }
}
