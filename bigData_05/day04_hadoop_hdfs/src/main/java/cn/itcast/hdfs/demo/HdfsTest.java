package cn.itcast.hdfs.demo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsTest {

    @Test
    public void getFileStstem1() throws IOException {
        Configuration configuration = new Configuration();

        //指定我们使用的文件系统
        configuration.set("fs.defaultFS", "hdfs://node01:8020/");
        //获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void getFileStstem2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());

        System.out.println("fileSystem:"+fileSystem);
    }

    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020" );
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println(fileSystem.toString());
    }

    /*
    *小文件合并上传
    */
    @Test
    public void mergeFile() throws  Exception{
        //获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.100:8020"), new Configuration(),"root");
        FSDataOutputStream outputStream = fileSystem.create(new Path("/bigfile.txt"));
        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //通过本地文件系统获取文件列表，为一个集合
        FileStatus[] fileStatuses = local.listStatus(new Path("file:///F:\\input"));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream,outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();
    }

    /*
    *下载文件
    */
    @Test
    public void getFileToLocal() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        FSDataInputStream inputStream = fileSystem.open(new Path("/a.txt"));
        FileOutputStream outputStream = new FileOutputStream(new File("F:\\aa.txt"));
        IOUtils.copy(inputStream,outputStream );
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();

    }

    /*
    *下载文件
    */
    @Test
    public void getConfig() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        fileSystem.copyToLocalFile(new Path("/config/core-site.xml"),new Path("file://f:/core-site.xml") );
        fileSystem.close();
    }





    /*
    *hdfs文件上传
    */
    @Test
    public void putData() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        fileSystem.copyFromLocalFile(new Path("file:///f:\\aa.txt"), new Path("/hahaha/mylalala/test"));

        fileSystem.close();
    }


    /*
    *hdfs上创建文件夹
    */
    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        boolean mkdirs = fileSystem.mkdirs(new Path("/hahaha/mylalala/test"));
        fileSystem.close();
    }
    /*
    *遍历 HDFS 中所有文件
    */
    @Test
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //获取fileSystem类
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        //获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);

        while(locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println(locatedFileStatus.getPath().toString());
        }
        fileSystem.close();

    }

    /*
    *获取FileStreamSystem
    *
    */
    @Test
    public void test1() throws IOException {

        //第一步：注册hdfs 的url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //获取文件输入流
        InputStream inputStream = new URL("hdfs://node01:8020/a.txt").openStream();
       //获取文件输出流
        FileOutputStream outputStream = new FileOutputStream(new File("F:\\hello.txt"));


        //实现文件的拷贝
        org.apache.commons.io.IOUtils.copy(inputStream,outputStream );

        //关闭流
        org.apache.commons.io.IOUtils.closeQuietly(inputStream);
        org.apache.commons.io.IOUtils.closeQuietly(outputStream);

    }
}


