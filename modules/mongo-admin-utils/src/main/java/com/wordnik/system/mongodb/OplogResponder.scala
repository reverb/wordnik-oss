// Copyright (C) 2010  Wordnik, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
//
package com.wordnik.system.mongodb

import collection.immutable
import collection.mutable
import java.io.IOException

import scala.collection.JavaConverters._
import com.mongodb.{ DBCollection, BasicDBObject }
import com.wordnik.persistence.DatabaseSchema

/**
 * User: will
 * Date: 2002-12-03
 */

// A responder can have a set of functions keyed to namespace, like 'smartwords.clickLog', it takes
// the BasicDBObject of the insert, update, or delete operation
// Note that what gets passed is the basic DB object that is updated, deleted, or inserted (not the entire)
// oplog record
//
// example use:
//
// val oplog = DBCollection = MongoDBConnectionManager.getOplog(databaseSchema)
// new OplogResponder(oplog, List("word")).
//  addInsertTrigger("word.dictionary_entry", RelatedWordsTrigger.addRelatedWords).
//  addUpdateTrigger("word.dictionary_entry", RelatedWordsTrigger.updateRelatedWords).
//  start()

// @param oplog - DBCollection (specifically, an oplog)
// @param databasesToInclude - which databases or collections to include (can further limit to collections)
// @param databasesToExclucde - which databases to exclude
//
case class OplogResponder(
  oplog: DBCollection,
  databasesToInclude: List[String],
  databasesToExclude: List[String] = List[String]())
    extends OplogRecordProcessor {

  @throws(classOf[Exception])
  def processRecord(dbo: BasicDBObject) {
    val childDbo = dbo.get("o").asInstanceOf[BasicDBObject]
    val ns = dbo.get("ns").toString
    val op = dbo.get("op").toString

    op match {
      case "i" ⇒ processInserts(childDbo, ns)
      case "u" ⇒ processUpdates(childDbo, ns)
      case "d" ⇒ processDeletes(childDbo, ns)
      case _   ⇒
    }
  }

  @throws(classOf[IOException])
  def close(string: String) {}

  private val insertTriggersT = new mutable.HashMap[String, immutable.HashSet[BasicDBObject ⇒ Unit]]

  def addInsertTrigger(key: String, function: BasicDBObject ⇒ Unit) =
    {
      insertTriggersT(key) = insertTriggersT.getOrElse(key, new immutable.HashSet[BasicDBObject ⇒ Unit]) + function
      this
    }

  def deleteInsertTrigger(key: String, function: BasicDBObject ⇒ Unit) =
    {
      insertTriggersT(key) = insertTriggersT.getOrElse(key, new immutable.HashSet[BasicDBObject ⇒ Unit]) - function
      this
    }

  def processInserts(dbo: BasicDBObject, ns: String) {
    insertTriggersT.getOrElse(ns, None) match {
      case fns: immutable.HashSet[BasicDBObject ⇒ Unit] ⇒ fns.foreach(fn ⇒ fn(dbo))
      case _ ⇒
    }
    this
  }

  def insertTriggers = insertTriggersT

  private val updateTriggersT = new mutable.HashMap[String, immutable.HashSet[BasicDBObject ⇒ Unit]]

  def addUpdateTrigger(key: String, function: BasicDBObject ⇒ Unit) =
    {
      updateTriggersT(key) = updateTriggersT.getOrElse(key, new immutable.HashSet[BasicDBObject ⇒ Unit]) + function
      this
    }

  def deleteUpdateTrigger(key: String, function: BasicDBObject ⇒ Unit) =
    {
      updateTriggersT(key) = updateTriggersT.getOrElse(key, new immutable.HashSet[BasicDBObject ⇒ Unit]) - function
      this
    }

  def processUpdates(dbo: BasicDBObject, ns: String) {
    updateTriggersT.getOrElse(ns, None) match {
      case fns: immutable.HashSet[BasicDBObject ⇒ Unit] ⇒ fns.foreach(fn ⇒ fn(dbo))
      case _ ⇒
    }
    this
  }

  def updateTriggers = updateTriggersT

  private val deleteTriggersT = new mutable.HashMap[String, immutable.HashSet[BasicDBObject ⇒ Unit]]

  def addDeleteTrigger(key: String, function: BasicDBObject ⇒ Unit) =
    {
      deleteTriggersT(key) = deleteTriggersT.getOrElse(key, new immutable.HashSet[BasicDBObject ⇒ Unit]) + function
      this
    }

  def deleteDeleteTrigger(key: String, function: BasicDBObject ⇒ Unit) =
    {
      deleteTriggersT(key) = deleteTriggersT.getOrElse(key, new immutable.HashSet[BasicDBObject ⇒ Unit]) - function
      this
    }

  def processDeletes(dbo: BasicDBObject, ns: String) {
    deleteTriggersT.getOrElse(ns, None) match {
      case fns: immutable.HashSet[BasicDBObject ⇒ Unit] ⇒ fns.foreach(fn ⇒ fn(dbo))
      case _ ⇒
    }
    this
  }

  def deleteTriggers = deleteTriggersT

  def start() = {
    val tailThread = new OplogTailThread(this, oplog)
    tailThread.setInclusions(databasesToInclude.asJava)
    tailThread.setExclusions(databasesToExclude.asJava)
    tailThread.start()

    //	start a stop-file monitor
    new StopFileMonitor(tailThread).start()
    this
  }

}

