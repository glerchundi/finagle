package com.twitter.finagle.neo4j

case class ServerError(code: String, message: String) extends Exception(message)
case class ClientError(message: String) extends Exception(message)
