package com.wordnik.mongo.connection

import com.mongodb._
import scala.collection.JavaConversions._
import runtime.ScalaRunTime
import collection.mutable
import org.slf4j.LoggerFactory
import annotation.switch

object SchemaType {
  val READ_ONLY = 1
  val READ_WRITE = 2
}

object MongoDBConnectionManager {

  val Logger = LoggerFactory.getLogger(MongoDBConnectionManager.getClass.getName.replaceAll("\\$$", ""))
  val mongos = new mutable.HashMap[String, Member]
  val pool = new mutable.HashMap[String, List[DBServer]].withDefaultValue(Nil)

  @deprecated("Use the Logger property instead.", "1.3.0")
  val LOGGER = Logger
  @throws(classOf[PersistenceException])
  def getConnection(schemaName: String, schemaType: Int): DB = {
    Logger.debug("getting from, key: " + schemaName + ", schemaType: " + schemaType)

    val snl = schemaName.toLowerCase
    if (!pool.contains(snl)) throw new PersistenceException("no configurations found for " + schemaName)

    def authenticatedWithReadPref(server: DBServer): DBServer = {
      if (server.username.isDefined && !server.db.isAuthenticated) {
        server.db.authenticate(server.username.get, server.password.toCharArray)
      } else if (server.username.isEmpty && schemaType == SchemaType.READ_ONLY) {
        server.db.setReadPreference(ReadPreference.secondaryPreferred)
      }
      server
    }

    val output: Option[DB] = {
      val rwOut = (for {
        server <- pool(snl)
        if server.replicationType == Member.RS || server.replicationType == Member.M
      } yield authenticatedWithReadPref(server)).headOption.map(_.db)

      if (schemaType == SchemaType.READ_ONLY && rwOut.isEmpty) pool(snl).headOption.map(_.db)
      else rwOut
    }

    output getOrElse (throw new PersistenceException("no configurations found for " + schemaName))
  }

  private[this] def oplogName(replicationType: Int) = if (replicationType == Member.RS) "oplog.rs" else "oplog.$main"
  def getOplog(friendlyName: String, host: String, username: String, password: String): Option[DBCollection] = {
    getConnection(friendlyName, host, "local", username, password, SchemaType.READ_WRITE)
    pool.get(friendlyName) flatMap {
      _.find(_.replicationType < Member.S) map (s => s.db.getCollection(oplogName(s.replicationType)))
    }
  }

  @throws(classOf[PersistenceException])
  def getConnection(schemaName: String, h: String, schema: String, u: String, pw: String, schemaType: Int): DB = {
    if (h.indexOf(":") > 0) getConnection(schemaName, h.split(":")(0), h.split(":")(1).toInt, schema, u, pw, schemaType)
    else getConnection(schemaName, h, 27017, schema, u, pw, schemaType)
  }

  @throws(classOf[PersistenceException])
  def getConnection(friendlyName: String, h: String, p: Int, schema: String, u: String, pw: String, schemaType: Int): DB = {
    val host: String = if (h == null || h.trim.isEmpty) "127.0.0.1" else h
    val port = if (p <= 0) 27017 else p
    val username = if (u != null && u.trim.nonEmpty) Some(u) else None
    val password = if (pw == null) "" else pw

    try {
      Logger.debug("getting connection to " + host + ":" + port + "/" + schema + " with username: " + username + ", password: " + password)

      val schemaId = (host + ":" + port + ":" + schema).toLowerCase
      mongos.get(schemaId) map { server =>
        Logger.debug("getting " + schemaId + " from map")
        val db = server.mongo.getDB(schema)
        val replicationType = detectReplicationType(db, username, password)
        Logger.debug("all known servers: " + db.getMongo.getServerAddressList)
        addServer(friendlyName, schema, db, username, password, replicationType)
      } getOrElse {
        Logger.debug("creating " + schemaId)
        val sa = new ServerAddress(host, port)
        val single = new Mongo(sa)

        val singleDb = single.getDB(schema)
        val replicationType = detectReplicationType(singleDb, username, password)

        val mongo = if (replicationType == Member.RS) new Mongo(List(sa)) else single
        val db = mongo.getDB(schema)
        mongos += schemaId -> new Member(replicationType, mongo)
        Logger.debug("schemaName: " + friendlyName + ", schemaId: " + schemaId)
        addServer(friendlyName, schema, db, username, password, replicationType)
      }
    } catch {
      case e: Exception => {
        Logger.error("can't get connection to " + host + ":" + port + "/" + schema + " with username " + username + ", password " + password)
        throw new PersistenceException(e)
      }
    }
  }

  def detectReplicationType(db: DB, username: Option[String], password: String) = {
    try {
      username foreach (db.authenticate(_, password.toCharArray))
      val stat = db.command("isMaster")
      if (stat.containsField("setName") && username.isEmpty) {
        Logger.debug("detected replica set")
        Member.RS
      } else if (stat.containsField("ismaster")) {
        if (stat.getBoolean("ismaster" , false)) Member.M
        else Member.S
      } else Member.UNKNOWN
    } catch {
      case e: Exception =>
        Logger.warn("Unable to detect the replication type, falling back to unknown", e)
        Member.UNKNOWN
      case _: Throwable =>
        throw new PersistenceException("Failed to detect replication type")
    }
  }

  def addServer(friendlyName: String, schema: String, db: DB, username: Option[String], password: String, replicationType: Int): DB = {
    val snl = friendlyName.toLowerCase
    val serverList = pool(snl)

    val dbServer = new DBServer(db, username, password, replicationType)
    pool += snl -> (if (!serverList.contains(dbServer)) dbServer :: serverList else serverList)
    Logger.debug("adding to pool, key: " + snl + ", db: " + db)
    db
  }
}

case class DBServer(db: DB, username: Option[String], password: String, replicationType: Int) {

  override def toString: String = {
    new mutable.StringBuilder().append("db: ").append(db).append(", replType: ").append(replicationType).toString
  }

  override def hashCode(): Int = ScalaRunTime._hashCode((db.getMongo.getAddress, replicationType))

  override def equals(other: Any): Boolean = {
    other match {
      case that: DBServer =>
        db.getMongo.getAddress == that.db.getMongo.getAddress && replicationType == that.replicationType
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

case class Member(memberType: Int, mongo: Mongo)