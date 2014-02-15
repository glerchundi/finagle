package com.twitter.finagle.neo4j.protocol.endpoints.transaction

import com.twitter.finagle.neo4j.protocol.{Transaction, SimpleRequest}
import com.twitter.finagle.http.Method._
import com.twitter.finagle.neo4j.util.MapperUtils
import TransactionRequests._

private[transaction] object TransactionRequests {

  case class JsonStatements(statements: List[JsonStatement])
  case class JsonStatement(statement: String, parameters: Option[JsonStatementParams] = None)
  case class JsonStatementParams(props: Map[String, Any])

  def encodeQueryAndParams(query: String, params: Map[String, Any] = Map()): String = {
    val statements = JsonStatements(
      if (query.isEmpty)
        List()
      else
        List(
          JsonStatement(
            query,
            if (params.isEmpty) None else Some(JsonStatementParams(params))
          )
        )
    )
    MapperUtils.writeJson(statements).get()
  }

  val emptyStatements = MapperUtils.writeJson(JsonStatements(List())).get()

}

case class Cipher(query: String,
                  params: Map[String, Any])
  extends SimpleRequest("/transaction", Post, encodeQueryAndParams(query, params))

case class CipherAndCommit(query: String,
                           params: Map[String, Any])
  extends SimpleRequest("/transaction/commit", Post, encodeQueryAndParams(query, params))

case class CipherUsingTx(tx: Transaction,
                         query: String,
                         params: Map[String, Any])
  extends SimpleRequest("/transaction/" + tx.id.toString, Post, encodeQueryAndParams(query, params))

case class CipherAndCommitUsingTx(tx: Transaction,
                                  query: String,
                                  params: Map[String, Any])
  extends SimpleRequest("/transaction/" + tx.id.toString + "/commit", Post, encodeQueryAndParams(query, params))

case class BeginTx()
  extends SimpleRequest("/transaction", Post, emptyStatements)

case class CommitTx(tx: Transaction)
  extends SimpleRequest("/transaction/" + tx.id.toString + "/commit", Post, emptyStatements)

case class RollbackTx(tx: Transaction)
  extends SimpleRequest("/transaction/" + tx.id.toString, Delete, "")

case class ResetTxTimeout(tx: Transaction)
  extends SimpleRequest("/" + tx.toString, Post, emptyStatements)