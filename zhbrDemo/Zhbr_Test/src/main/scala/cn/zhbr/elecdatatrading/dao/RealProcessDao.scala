package cn.zhbr.elecdatatrading.dao

import java.text.SimpleDateFormat
import java.util.Date

import cn.zhbr.elecdatatrading.entity.PrincipalMessage
import com.zhbr.scalautils.hbaseutils.HbaseUtil
import com.zhbr.scalautils.mysqlutils.MysqlManager
import com.zhbr.scalautils.redisutils.JedisConnectionPool
import kafka.common.TopicAndPartition
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

class RealProcessDao {

  /**
    * 数据写入MySQL
    * @param partition
    */
  def saveBatchPrincipalMessageToMySQL(partition: Iterator[(String, PrincipalMessage)]) = {
    val sql =
      "insert into MT_TRADE_SEQ (SEQ_ID, TRADE_SEQ_CODE, TRADE_SEQ_NAME, CREATOR_ID, CREATE_TIME) values (?,?,?,?,DATE_FORMAT(?,'%Y-%m-%d %H:%i:%S')) ;"
    // 模拟参数　数组
    var paramList:ArrayBuffer[Array[Any]] = new ArrayBuffer()
    // 组拼　每个partition　中的数据。
    partition.foreach( data => {
      val principalMessage = data._2
      val currTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
      // 封装　MySQL 数据
      val params= Array[Any](principalMessage.SEQ_ID, principalMessage.TRADE_SEQ_CODE, principalMessage.TRADE_SEQ_NAME, principalMessage.CREATOR_ID, currTime)
      paramList += params
    })
    // 调用 MySQL 的　api　进行插入数据　操作。
    MysqlManager.getMysqlManager.batchInsertWithParam(sql, paramList)
    logger.warn(s"010101010101010101010101010101010101010 time elapsed: ")
  }

  /**
    * 数据写入Redis
    * @param partation
    */
  def saveBatchPrincipalMessageToRedis(partition: Iterator[(String, PrincipalMessage)]): Unit = {
    val resultSet = ArrayBuffer[Map[String, String]]()
    // 组拼　每个partition　中的数据。
    partition.foreach( data => {
      val key = data._1
      val principalMessage = data._2

      // 封装　Redis 的　Map类型数据，　　　待　重构
      val map = scala.collection.mutable.Map[String,String]()
      map("key")=key
      map("SEQ_ID")=principalMessage.SEQ_ID
      map("TRADE_SEQ_CODE") = principalMessage.TRADE_SEQ_CODE
      map("TRADE_SEQ_NAME") = principalMessage.TRADE_SEQ_NAME
      map("CREATOR_ID") = principalMessage.CREATOR_ID
      val currTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
      map("CREATE_TIME") = currTime

      resultSet.append(map.toMap)
    })
    JedisConnectionPool.batchSetMap(resultSet.toSeq)
    logger.error(" 000000000000000000000000000000000000   saveBatchDataToRedis")
  }






  @transient lazy val logger = Logger.getLogger(this.getClass)


  /**
    * 过滤CREATOR_ID为空值的数据
    * @param message
    * @return
    */
  def filterData(principalMessage: (String,PrincipalMessage)): Boolean = {
    val value: PrincipalMessage = principalMessage._2
    val CREATOR_ID = value.CREATOR_ID

    print(CREATOR_ID + "-----------CREATOR_ID--------------------")

        if (StringUtils.isNotEmpty(CREATOR_ID)) {
          //如果能进来,匹配上
          return true
        }
        false
    }




  /**
    * 把 数据存入HBase
    * @param resultDStream
    */
  def savePrincipalMessageToHBase(resultDStream: DStream[(String, PrincipalMessage)]): Unit ={
    resultDStream.foreachRDD(rdd =>{
      val putRDDs: RDD[ (ImmutableBytesWritable, Put) ] = rdd.map(data => {
        // 转换　格式
        convert4PrincipalMessage(data._1, data._2)
      })
      // 调用HBase批量保存数据，
      HbaseUtil.putRDD(putRDDs,hbaseTableName)
    })
  }


  val hbaseTableName = "category_clickcount222"
  /**
    * 把 数据存入HBase
    * @param filterData
    */
  def savePrincipalMessageToHBase1(filterData: RDD[ (String, PrincipalMessage) ]): Unit ={
      val putRDDs: RDD[ (ImmutableBytesWritable, Put) ] = filterData.map(data => {
        // 转换　格式
        convert4PrincipalMessage(data._1, data._2)
      })
      // 调用HBase批量保存数据，
      HbaseUtil.putRDD(putRDDs,hbaseTableName)
  }



  /**
    * 构建转换数据
    * @param principalMessage
    * @return
    */
  def convert4PrincipalMessage(rowkey:String, principalMessage:PrincipalMessage) = {
    println("convert4PrincipalMessage  ----  "+principalMessage.toString)
    var put:Put = null
    try{
      put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CREATOR_ID"), Bytes.toBytes(principalMessage.CREATOR_ID))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SEQ_ID"), Bytes.toBytes(principalMessage.SEQ_ID))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("TRADE_SEQ_CODE"), Bytes.toBytes(principalMessage.TRADE_SEQ_CODE))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("TRADE_SEQ_NAME"), Bytes.toBytes(principalMessage.TRADE_SEQ_NAME))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(principalMessage.CREATE_TIME))
    }catch{
      case e:Exception=>println(">>> principalMessage 数据转换失败："+e)
    }
    (new ImmutableBytesWritable, put)
  }


  /**
    * 从　MySQL　中读取　Offset。
    * @return
    */
  def readOffsetFromMySQL:  List[(String, Int, Int)] = {
    val offsetList = ArrayBuffer[(String, Int, Int)]()

    val sql = "SELECT topic, partitionname, untilOffset FROM offsetinfo;"
    val dbName = "test"
    val maps = MysqlManager.getMysqlManager.executeQueryNoParams(sql, dbName)
    for(map <- maps){
      println("map: "+map.toString()+"  ----------------")
      val topic = map("TOPIC").toString
      val partition = map("PARTITIONNAME").toString.toInt
      val untilOffset = map("UNTILOFFSET").toString.toInt

      offsetList.append((topic, partition, untilOffset))
    }
    offsetList.toList
  }

  /**
    * 将外部　存储的　kafka　offset　，转换成　Map[TopicAndPartition, Long]　格式。
    * @param list
    * @return
    */
  def transformOffsetsFormat(list: List[(String, Int, Int)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2) //topic和分区数
      fromOffsets += (tp -> offset._3) // offset位置
    }
    fromOffsets
  }
}
