package cn.zhbr.elecdatatrading.entity

/**
 * 交易序列表--引用交易组织模块的的交易序列表
 *
 * 主体信息topic的消息使用Json格式，结构如下（相关字段名需要按最终的物理表设计确定）：
 * {
 * SEQ_ID：
 * TRADE_SEQ_CODE：
 * TRADE_SEQ_NAME：
 * CREATOR_ID：
 * CREATE_TIME：
 * ........
 * },
 *
 * CREATE TABLE `MT_TRADE_SEQ` (
 * `SEQ_ID` varchar(42) NOT NULL COMMENT '交易序列标识',
 * `TRADE_SEQ_CODE` varchar(42) DEFAULT NULL COMMENT '交易序列编码',
 * `TRADE_SEQ_NAME` varchar(30) DEFAULT NULL COMMENT '交易序列名称',
 * `CREATOR_ID` varchar(42) DEFAULT NULL COMMENT '创建人',
 * `CREATE_TIME` datetime DEFAULT NULL COMMENT '创建时间',
 * PRIMARY KEY (`SEQ_ID`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='交易序列表--引用交易组织模块的的交易序列表';
 *
 */
case class PrincipalMessage( SEQ_ID:String,  // '交易序列标识'
                             TRADE_SEQ_CODE:String,  // '交易序列编码'
                             TRADE_SEQ_NAME:String,  // '交易序列名称'
                             CREATOR_ID:String,  // '创建人'
                             CREATE_TIME:String  // '创建时间'
                           ) extends Serializable {

}
