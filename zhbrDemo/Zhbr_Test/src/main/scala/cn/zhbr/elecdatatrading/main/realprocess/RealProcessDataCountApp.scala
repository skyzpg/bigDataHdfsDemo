package cn.zhbr.elecdatatrading.main.realprocess

import cn.zhbr.elecdatatrading.entity.PrincipalMessage
import cn.zhbr.elecdatatrading.service.RealProcessService
import cn.zhbr.elecdatatrading.utils.CalcRulesUtils
import com.zhbr.scalautils.sparkutil.BroadcastWrapper
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* 实时数据处理   通过简单计算后  将结果存入MySQL和Redis
* */
object RealProcessDataCountApp {

  def main(args: Array[ String ]): Unit = {

    @transient lazy val logger = Logger.getLogger(this.getClass)
    lazy val service = new RealProcessService

    //1 构建ssc
    val conf = new SparkConf()
      .setAppName("RealProcessDataCountApp")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    //2 获取数据  从kafka获取数据
    val kafkaDataStream: InputDStream[ (String, String) ] = service.getDirectStream(ssc)
    //val inputStream: DStream[ String ] = kafkaDataStream.map(_._2)

    //3 加载广播变量中的临时变量
    val rulesMap: Map[String, Object ] = CalcRulesUtils.getRules()
    val rulesBroadcast: BroadcastWrapper[ (Long, Map[ String, Object ]) ] = BroadcastWrapper(ssc,(System.currentTimeMillis(),rulesMap))
    val updateFreq = 10000 //5min  设置每五分钟自动更新一次广播变量
    //4 数据计算 ，依据MySQL中加载的计算规则
    val resultData: DStream[ (String, PrincipalMessage) ] = service.CalculateIndexesByRule(kafkaDataStream,rulesBroadcast,updateFreq)

    //5 结果落地   mysql和redis中都要存储
    service.storeResultData(resultData)


    //6 更新offset
    service.storeKafkaOffsets2MySQL(kafkaDataStream)

    //7 执行程序
    ssc.start()
    ssc.awaitTermination()
  }
}
