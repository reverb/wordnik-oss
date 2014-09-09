package com.wordnik.mongo.connection

case class PersistenceException(message: String, causedBy: Exception) extends Exception(message, causedBy)

object PersistenceException {
  def apply(msg: String): PersistenceException = PersistenceException(msg, null)
}
