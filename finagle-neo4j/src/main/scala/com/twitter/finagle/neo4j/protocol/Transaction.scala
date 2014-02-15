package com.twitter.finagle.neo4j.protocol

case class Transaction(commit: String,
                       expires: String) {
  val id: Int = {
    val slashIdx = commit.lastIndexOf('/')
    commit
      .substring(slashIdx, commit.length - slashIdx)
      .toInt
  }
}
