package cn.zhbr.elecdatatrading.service

import cn.zhbr.elecdatatrading.dao.RealProcessDao
import cn.zhbr.elecdatatrading.entity.PrincipalMessage
import cn.zhbr.elecdatatrading.utils.{CalcRulesUtils, ParseJsonUtil}
import com.zhbr.scalautils.confutils.ConfUtil
import com.zhbr.scalautils.sparkutil.BroadcastWrapper
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import scalikejdbc._
import scalikejdbc.config.DBs

/*
*  实时数据处理的service类
* */
class RealProcessService {


  @transient lazy val logger = Logger.getLogger(this.getClass)
  lazy val dao = new RealProcessDao


  /**
    * 将数据存入MySQL和Redis
    * @param resultData
    */
  def storeResultData(resultData: DStream[(String, PrincipalMessage)]): Unit = {

    resultData.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          partition=>{
            //批量保存到MySQL
            dao.saveBatchPrincipalMessageToMySQL(partition)
            //批量保存到Redis
            dao.saveBatchPrincipalMessageToRedis(partition)
          })
      })
  }


  /**
    *
    * @param kafkaDataStream inputSteam
    * @param rulesBroadcast 广播变量
    * @param updateFreq  更新时间
    * @return resultDataStream 计算结果
    */
  def CalculateIndexesByRule(kafkaDataStream: InputDStream[(String, String)], rulesBroadcast: BroadcastWrapper[(Long, Map[String, Object])], updateFreq: Int): DStream[ (String, PrincipalMessage) ] = {
    //1 根据计算公式计算相关指标
    val resultDataStream: DStream[ (String, PrincipalMessage) ] = kafkaDataStream.map(line => {
      // 解析　json　格式字符串,  生成　principalMessage 对象
      ParseJsonUtil.json2PrincipalMessage(line._2)
    }).transform(rdd => {
      //  检查更新时差，动态更新规则
      if (System.currentTimeMillis() - rulesBroadcast.value._1 > updateFreq) {
        rulesBroadcast.update((System.currentTimeMillis, CalcRulesUtils.getRules), true)
        logger.warn("[BroadcastWrapper] rules updated")
      }
      // 提取规则
      val rules = rulesBroadcast.value._2

      // 开始计算
      rdd.map(record => {
        val aaaRule: Object = rules("aaa")
        val TRADE_SEQ_NAME: String = record.TRADE_SEQ_NAME
        (record.SEQ_ID + "--" + aaaRule, record)
      })
    })
     return resultDataStream
    // * 4.更新广播变量，（可以选择手动更新MySQL中的值
  }



  /**
    * 存储　kafka 的　offset　到外部存储。
    * 适用于　手动控制　kafka－offset
    * @param stream
    */
  def storeKafkaOffsets2MySQL(stream: InputDStream[(String, String)]): Unit ={
    // messages 从kafka获取数据,将数据转为RDD
    stream.foreachRDD((rdd, batchTime) => {
      import org.apache.spark.streaming.kafka.HasOffsetRanges
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges   // 获取偏移量信息
      /**
        * OffsetRange 是对topic name，partition id，fromOffset(当前消费的开始偏移)，untilOffset(当前消费的结束偏移)的封装。
        * *  所以OffsetRange 包含信息有：topic名字，分区Id，开始偏移，结束偏移
        */
      println("===========================> count: " + rdd.map(x => x + "1").count())
      // offsetRanges.foreach(offset => println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))
      for (offset <- offsetRanges) {
        // 遍历offsetRanges,里面有多个partition
        println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)
        // 将　offsets 写入到　MySQL　中。
        writeOffsetToMySQL(offset: OffsetRange)
      }
    })
  }


  /**
    * 将　Offset　写入到MySQL　中。
    * @return
    */
  def writeOffsetToMySQL(offset: OffsetRange) = {
    DBs.setupAll()
    // 将partition及对应的untilOffset存到MySQL中
    val saveoffset = DB localTx {
      implicit session =>
        sql"DELETE FROM offsetinfo WHERE topic = ${offset.topic} AND partitionname = ${offset.partition}".update.apply()
        sql"INSERT INTO offsetinfo (topic, partitionname, untilOffset) VALUES (${offset.topic},${offset.partition},${offset.untilOffset})".update.apply()
    }
  }


  /**
    * 将数据进行简单过滤后直接存入HBase
    * @param kafkaDataStream
    */
  def filterAndStoreDataToHBase(kafkaDataStream: InputDStream[(String, String)]) = {

    val value: DStream[ (String, PrincipalMessage) ] = kafkaDataStream.map(line => {
      // 解析　json　格式字符串,  生成　principalMessage 对象
      val principalMessage: PrincipalMessage = ParseJsonUtil.json2PrincipalMessage(line._2)
      // 拼接　rowkey
      val rowkey = constituteRowKey(principalMessage) // 封装为(key,principalMessage)格式化的　Tuple
      (rowkey, principalMessage)
    })

    value.foreachRDD(
      //过滤空值
      PrincipalMessageRDD=>{
        val filterData: RDD[ (String, PrincipalMessage) ] = PrincipalMessageRDD
          .filter(
            principalMessage => (dao.filterData(principalMessage)))

        //数据存入HBase
        dao.savePrincipalMessageToHBase1(filterData)
    })
  }


  /**
    * 组拼RowKey
    * @param principalMessage
    * @return
    */
  def constituteRowKey(principalMessage: PrincipalMessage): String = {
    println(principalMessage.SEQ_ID+"-- 组拼　rowkey --"+principalMessage.TRADE_SEQ_NAME)
    val rowkey = "rowkey111111111111111"
    rowkey
  }




  /**
    * 从kafka获取数据
    * @param ssc
    * @return inputStream
    */
  def getDirectStream(ssc: StreamingContext):InputDStream[(String, String)] = {

    //获取inputStream之前，先到MySQL中查询offset
    val offsetList: List[(String, Int, Int)] = dao.readOffsetFromMySQL

    //将外部存储的kafka　offset　，转换成　Map[TopicAndPartition, Long]　格式
    val offsetMap: Map[TopicAndPartition, Long ] = dao.transformOffsetsFormat(offsetList)

    val dauMessageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

    val size = offsetList.size

    val inputStream: InputDStream[(String, String)] =  if (size > 0) {
      //若offset有值  则接着偏移量消费
      //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc, ConfUtil.KAFKA_PARAMS , offsetMap, dauMessageHandler)
    }
      //若offset没有值  则从头开始消费
    else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, ConfUtil.KAFKA_PARAMS, ConfUtil.KAFKA_TOPICS)
    }
    inputStream
  }










}
