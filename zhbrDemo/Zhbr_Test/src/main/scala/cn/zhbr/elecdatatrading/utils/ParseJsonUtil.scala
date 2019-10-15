package cn.zhbr.elecdatatrading.utils

import cn.zhbr.elecdatatrading.entity.PrincipalMessage
import spray.json._

object PrincipalMessageJsonProtocol extends DefaultJsonProtocol with Serializable {
  implicit val docFormat = jsonFormat5(PrincipalMessage)
}




/**
 * 解析　Json 字符串
 */
object ParseJsonUtil extends Serializable {
  def main(args: Array[String]): Unit = {

//    val myDomain2 = MyDomain1("rowkey:1111","SEQ_ID1111111","TRADE_SEQ_CODE11111",new DateTime().toDateTime.toString("yyyy-MM-dd hh-mm-ss"))
//    val objJsonStr = myDomain2Json(myDomain2)
//    println("myDomain2 == "+objJsonStr)
    val objJsonStr =""
    val myDomain = json2PrincipalMessage(objJsonStr)
    println("json2Record = "+myDomain.toString)

  }



  /**
   * 解析ＪＳＯＮ为　对象
   * @param json
   * @return
   */
  def json2PrincipalMessage(json: String): PrincipalMessage = {
    import PrincipalMessageJsonProtocol._
    val obj = json.parseJson.convertTo[PrincipalMessage]
    obj
  }

}


