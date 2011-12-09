package com.wordnik.mongo.connection

class PersistenceException(msg: String, e: Exception) extends Exception {
  def this() = this(null, null)
  def this(msg: String) = this(msg, null)
  def this(e: Exception) = this(null, e)
}