package com.wordnik.system.mongodb

import collection.mutable
import com.mongodb.BasicDBObject
import java.io.IOException

/**
 * User: ramesh
 * Date: 7/16/12
 * Time: 8:06 AM
 */

class OpLogReader extends OplogRecordProcessor {

  val recordTriggers = new mutable.HashSet[Function1[BasicDBObject, Unit]]

  @throws(classOf[Exception])
  def processRecord(dbo: BasicDBObject) = {
    recordTriggers.foreach(t => t(dbo))
  }

  @throws(classOf[IOException])
  def close(string: String) = {

  }

}
