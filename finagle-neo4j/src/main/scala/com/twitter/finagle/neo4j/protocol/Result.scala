package com.twitter.finagle.neo4j.protocol

trait Result {
  val isError: Boolean = false
}