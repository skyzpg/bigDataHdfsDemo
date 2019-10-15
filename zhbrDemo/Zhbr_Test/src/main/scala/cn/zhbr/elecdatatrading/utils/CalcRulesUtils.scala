package cn.zhbr.elecdatatrading.utils

import com.zhbr.scalautils.mysqlutils.MysqlManager

import scala.collection.mutable

object CalcRulesUtils {

  def main(args: Array[String]): Unit = {
    val ruleMap: Map[String, Object ] = getRules
    println("22222222222222222222222222222222222222 ;;   "+ ruleMap.toString())

  }

  /**
   * 加载管理员和用户词库
   */
  def getRules(): Map[String, Object] = {
    val sql = "select name, addr from teacherinfo"
    val ruleMap: mutable.Map[String, Object ] = scala.collection.mutable.Map[String,  Object]()
    val maps = MysqlManager.getMysqlManager.executeQueryNoParams(sql, "test")
    for(m <- maps){
      val key = m("NAME").toString
      val value = m("ADDR")
      ruleMap(key)= value
      //      for ((k, v) <- m){
      //        println("k="+k+" , v="+v)
      //      }
    }
    ruleMap.toMap
  }
}
