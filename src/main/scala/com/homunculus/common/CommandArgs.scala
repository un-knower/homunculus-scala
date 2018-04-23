package com.homunculus.common

import com.beust.jcommander.Parameter

/**
 * CommandArgs
 *
 * @author Jade
 */
object CommandArgs {

  @Parameter(names = Array("-brokers"), required = true, description = "kafka brokers")
  var brokers: String = null

  @Parameter(names = Array("-topic"), required = true, description = "kafka topic")
  var topic: String = null

  @Parameter(names = Array("-groupId"), required = true, description = "kafka consumer id")
  var groupId: String = null

  @Parameter(names = Array("-interval"), description = "trigger")
  var interval: Long = 5000

  @Parameter(names = Array("-esNodes"), required = true)
  var esNodes: String = null

  @Parameter(names = Array("-esPort"), required = true)
  var esPort: String = null

  override def toString = s"CommandArgs(brokers=$brokers\n topic=$topic\n groupId=$groupId\n interval=$interval\n esNodes=$esNodes\n esPort=$esPort)"
}
