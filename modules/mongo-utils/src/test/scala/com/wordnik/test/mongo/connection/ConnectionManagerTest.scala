package com.wordnik.test.mongo.connection

import com.wordnik.mongo.connection._

import com.mongodb._

import org.junit.runner.RunWith

import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection.JavaConversions._

import scala.io._

@RunWith(classOf[JUnitRunner])
class ConnectionManagerTest extends FlatSpec with ShouldMatchers {
  it should "create a master connection" in {
    val db = MongoDBConnectionManager.getConnection(
      "test-ms", "localhost", 27017, "test", null, null, SchemaType.READ_WRITE)
    assert(db != null)
  }

  it should "get a master connection by friendly name" in {
    // get it from the pool
    val check = MongoDBConnectionManager.getConnection("test-ms", SchemaType.READ_WRITE)
    assert(check != null)
  }

  it should "get a connection to a replica set" in {
    val db = MongoDBConnectionManager.getConnection(
      "test-rs", "localhost", 27018, "test", null, null, SchemaType.READ_WRITE)
  }

  it should "get a replica set connection by friendly name" in {
    // get it from the pool
    val check = MongoDBConnectionManager.getConnection("test-rs", SchemaType.READ_WRITE)
    assert(check != null)
  }

  it should "test the MS oplog" in {
    val oplog = MongoDBConnectionManager.getOplog("oplog-ms", "localhost:27017", null, null).get
    assert(oplog != null)

    val cursor = oplog.find()
    cursor.addOption(Bytes.QUERYOPTION_TAILABLE)
    cursor.addOption(Bytes.QUERYOPTION_AWAITDATA)

    cursor.next
    println(MongoDBConnectionManager.pool)
  }

  it should "test the RS oplog" in {
    val oplog = MongoDBConnectionManager.getOplog("oplog-rs", "localhost:27019", null, null).get
    assert(oplog != null)

    val cursor = oplog.find()
    cursor.addOption(Bytes.QUERYOPTION_TAILABLE)
    cursor.addOption(Bytes.QUERYOPTION_AWAITDATA)

    cursor.next
    println(MongoDBConnectionManager.pool)
  }
}
