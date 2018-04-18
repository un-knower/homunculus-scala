package com.homunculus.common

/**
 * CanalModel
 *
 * @author Jade
 */
case class CanalModel(
                       //对象类型，例如，订单
                       var afterColumn: java.util.HashMap[String, String] = new java.util.HashMap[String, String],
                       var beforeColumn: java.util.HashMap[String, String] = new java.util.HashMap[String, String],
                       //对象id，例如，订单号
                       var columnsType: java.util.HashMap[String, String] = new java.util.HashMap[String, String],
                       //记录时间
                       var database: String = null,
                       //业务类型, 对象业务的操作类型
                       var dmlType: String = null,
                       //随机数
                       var executeTime: String = null,
                       //动作， 操作类型中文描述
                       val keys: String = null,
                       //内容
                       val table: String = null,
                       //说明
                       val updateFields: String = null
                     )
