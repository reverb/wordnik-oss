package com.wordnik.mongo.connection

import com.mongodb._

import java.net.UnknownHostException
import java.util.StringTokenizer
import java.util.logging.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable._

import scala.util.control._

object SchemaType {
  val READ_ONLY = 1
  val READ_WRITE = 2
}

object MongoDBConnectionManager {
  val LOGGER = Logger.getLogger(MongoDBConnectionManager.getClass.getName)
  val mongos = new HashMap[String, Member]
  val pool = new HashMap[String, List[DBServer]]

  @throws(classOf[PersistenceException])
  def getConnection(schemaName: String, schemaType: Int): DB = {
    LOGGER.finest("getting from, key: " + schemaName + ", schemaType: " + schemaType)

    val snl = schemaName.toLowerCase
    if (!pool.contains(snl)) throw PersistenceException("no configurations found for " + schemaName)

    val servers = pool(snl)
    var output: Option[DB] = None

    schemaType match {
      //	read write => rs or master
      case SchemaType.READ_WRITE => {
        servers.foreach(server => {
          if (server.replicationType == Member.RS || server.replicationType == Member.M) {
            server.username match {
              case Some(user) => {
                if (!server.db.isAuthenticated)
                  server.db.authenticate(user, server.password.toCharArray)
              }
              case _ =>
            }
            output = Some(server.db)
          }
        })
      }
      case _ => {
        //  slaves => rs or slave unless only one master
        servers.foreach(server => {
          if (server.replicationType == Member.RS || server.replicationType == Member.S) {
            server.username match {
              case Some(user) => {
                if (!server.db.isAuthenticated)
                  server.db.authenticate(user, server.password.toCharArray)
              }
              case _ => {
                server.db.setReadPreference(ReadPreference.secondaryPreferred)
              }
            }
            output = Some(server.db)
          }
        })
        //this means there are no slaves so it is ok to get connection from any server
        if (output == None) {
          //	user master anyway
          output = Some(servers(0).db)
        }
      }
    }

    output match {
      case Some(db) => db
      case _ => throw PersistenceException("no configurations found for " + schemaName)
    }
  }

  def getOplog(friendlyName: String, host: String, username: String, password: String): Option[DBCollection] = {
    val db = getConnection(friendlyName, host, "local", username, password, SchemaType.READ_WRITE)
    val servers = pool.get(friendlyName)
    var coll: Option[DBCollection] = None
    servers.get.foreach(server => {
      if (server.replicationType == Member.RS)
        coll = Some(server.db.getCollection("oplog.rs"))
      else if (server.replicationType == Member.M)
        coll = Some(server.db.getCollection("oplog.$main"))
    })
    coll
  }

  @throws(classOf[PersistenceException])
  def getConnection(schemaName: String, h: String, schema: String, u: String, pw: String, schemaType: Int): DB = {
    if (h.indexOf(",") > 0) {
      val hosts = h.split(",")
      var ret: DB = null
      val loop = new Breaks
      loop.breakable {
        for(oh <- hosts)
        {
          try {
            ret = getConnectionPrivate(schemaName, oh, schema, u, pw, schemaType)
            loop.break
          } catch {
            case e: PersistenceException => {
            }
          }
        }
      }
      if (ret == null) throw PersistenceException("no replica set member can be connected")
      else ret
    }
    else {
      getConnectionPrivate(schemaName, h, schema, u, pw, schemaType)
    }
  }

  @throws(classOf[PersistenceException])
  private def getConnectionPrivate(schemaName: String, h: String, schema: String, u: String, pw: String, schemaType: Int): DB = {
    if (h.indexOf(":") > 0) getConnection(schemaName, h.split(":")(0), h.split(":")(1).toInt, schema, u, pw, schemaType)
    else getConnection(schemaName, h, 27017, schema, u, pw, schemaType)
  }

  @throws(classOf[PersistenceException])
  def getConnection(friendlyName: String, h: String, p: Int, schema: String, u: String, pw: String, schemaType: Int): DB = {
    var host: String = h
    var port = p
    val username = {
      if (null != u && u.trim != "") Some(u)
      else None
    }
    val password = {
      if (null == pw) ""
      else pw
    }
    try {
      if (null == host) host = "localhost"
      if (port <= 0) port = 27017

      LOGGER.finest("getting connection to " + host + ":" + port + "/" + schema + " with username: " + username + ", password: " + password)

      val schemaId = (host + ":" + port + ":" + schema).toLowerCase
      val db = mongos.contains(schemaId) match {
        case true => {
          LOGGER.finest("getting " + schemaId + " from map")
          val db = mongos(schemaId).mongo.getDB(schema)
          val replicationType = detectReplicationType(db, username, password)
          LOGGER.finest("all known servers: " + db.getMongo.getServerAddressList)
          addServer(friendlyName, schema, db: DB, username, password, replicationType)
          db
        }
        case _ => {
          LOGGER.finest("creating " + schemaId)
          val sa = new ServerAddress(host, port)
          var mongo = new Mongo(sa)

          var db = mongo.getDB(schema)
          db.getStats() // Command as a connection check to make a connection to the server for availability check of the server
          val replicationType = detectReplicationType(db, username, password)

          if (replicationType == Member.RS) {
            mongo = new Mongo(List(sa))
            db = mongo.getDB(schema)
          }
          mongos += schemaId -> new Member(replicationType, mongo)
          LOGGER.finest("schemaName: " + friendlyName + ", schemaId: " + schemaId)
          addServer(friendlyName, schema, db: DB, username, password, replicationType)
          db
        }
      }
      db
    } catch {
      case e: Exception => {
        LOGGER.severe("can't get connection to " + host + ":" + port + "/" + schema + " with username " + username + ", password " + password);
        throw PersistenceException("can't get connection to " + host + ":" + port + "/" + schema + " with username " + username + ", password " + password, e);
      }
    }
  }

  def detectReplicationType(db: DB, username: Option[String], password: String) = {
    try {
      //	check it
      var stat: String = null
      username match {
        case Some(username) => {
          db.authenticate(username, password.toCharArray)
          stat = db.command("isMaster").toString
          if (stat.indexOf("\"ismaster\" : true") > 0) Member.M
          else Member.S
        }
        case _ => {
          stat = db.command("isMaster").toString
          if (stat.indexOf("setName") > 0) {
            //  is a replica set
            LOGGER.finest("detected replica set")
            Member.RS
          } else {
            if (stat.indexOf("\"ismaster\" : true") > 0) Member.M
            else if (stat.indexOf("\"ismaster\" : false") > 0) Member.S
            else Member.UNKNOWN
          }
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace
        Member.UNKNOWN
      }
      case _: Throwable =>
        throw PersistenceException("Failed to detect replication type")
    }
  }

  def addServer(friendlyName: String, schema: String, db: DB, username: Option[String], password: String, replicationType: Int) = {
    val snl = friendlyName.toLowerCase
    if (!pool.contains(snl)) {
      pool += snl -> List[DBServer]()
    }
    val serverList = new ListBuffer[DBServer]
    pool(snl).foreach(server => serverList += server)

    val dbServer = new DBServer(db, username, password, replicationType)
    if(!serverList.contains(dbServer)) serverList += dbServer
    pool += snl -> serverList.toList
    LOGGER.finest("adding to pool, key: " + snl + ", db: " + db)
  }
}

class DBServer(val db: DB, val username: Option[String], val password: String, val replicationType: Int) {

  override def toString: String = {
    new StringBuilder().append("db: ").append(db).append(", replType: ").append(replicationType).toString
  }

  override def equals(other:Any):Boolean = {
    other match {
      case that:DBServer => (this.db.getMongo.getAddress == that.db.getMongo.getAddress &&
                                this.replicationType == that.replicationType)
      case _ => false
    }
  }
}

object Member {
  val RS = 1
  val M = 2
  val S = 3
  val UNKNOWN = 4
}

class Member(val memberType: Int, val mongo: Mongo)
