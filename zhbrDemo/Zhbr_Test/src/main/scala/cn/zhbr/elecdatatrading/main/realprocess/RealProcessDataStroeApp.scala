package cn.zhbr.elecdatatrading.main.realprocess

import cn.zhbr.elecdatatrading.service.RealProcessService
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* 实时数据处理job1—————获取主体数据、清洗后存入HBase
*
* */

object RealProcessDataStroeApp {

  //1 构建ssc环境
   val conf = new SparkConf()
    .setAppName("RealProcessDataStroeApp")
    .setMaster("local[*]")

   val sc = new SparkContext(conf)
   val ssc = new StreamingContext(sc,Seconds(2))

  lazy val service = new RealProcessService


  //2 获取数据  从kafka中获取inputStream
   val kafkaDataStream: InputDStream[(String, String)] = service.getDirectStream(ssc)

  //3 简单清洗过滤  存入HBase
  service.filterAndStoreDataToHBase(kafkaDataStream)

  //4 数据存入成功后，更新offset到MySQL或者zk
  service.storeKafkaOffsets2MySQL(kafkaDataStream)

  //5 执行程序
  ssc.start()
  ssc.awaitTermination()
}
