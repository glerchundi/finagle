package com.twitter.finagle.neo4j

import com.twitter.finagle.neo4j.protocol.{Request, Result, Transaction}
import com.twitter.finagle.neo4j.protocol.endpoints.transaction._
import com.twitter.util.Future

trait TransactionEndpoint { self: BaseClient =>

  /**
   * Queries to Neo4j
   *
   * @param query A cypher statement that returns a result set.
   * @param params Statement parameters.
   * @param commit A boolean, if set, which commits the transaction.
   * @param txOpt An option containing the transaction in which
   *              this query should be executed.
   * @tparam T Output parameter type.
   * @return Partial function which permits to map Result into
   *         custom type
   */
   def cypher[T](query: String, params: Map[String, Any] = Map(), commit: Boolean = true)
               (implicit txOpt: Option[Transaction] = None): PartialFunction[Result, Future[T]] => Future[T] = {
    val request =
      (txOpt, commit) match {
        // No tx, Don't commit
        case (None,     false) => Cipher(query, params)
        // No tx, Commit
        case (None,      true) => CipherAndCommit(query, params)
        // Tx, Don't commit    =
        case (Some(tx), false) => CipherUsingTx(tx, query, params)
        // Tx, Commit
        case (Some(tx),  true) => CipherAndCommitUsingTx(tx, query, params)
      }
    doTxEndpointRequest(request)
  }

  /**
   * Sends a query that presumably returns a ResultSet.
   * Each row is mapped to f, a function from Row => T.
   *
   * @param query A cypher statement that returns a result set.
   * @param params Statement parameters.
   * @param commit A boolean, if set, which commits the transaction.
   * @param f A function from Row to any type T.
   * @param txOpt An option containing the transaction in which
   *              this query should be executed.
   * @tparam T Output parameter type.
   * @return a Future of Seq[T] that contains the result
   *         of f applied to each row.
   */
  def select[T](query: String, params: Map[String, Any] = Map(), commit: Boolean = true)
               (f: Row => T)
               (implicit txOpt: Option[Transaction] = None): Future[(Option[Transaction], Seq[T])] = {
    cypher(query, params, commit)(txOpt) {
      case rs: ResultSet => Future.value(rs.txOpt, rs.rows map f)
      case ok: Ok        => Future.value(ok.txOpt, Seq.empty)
    }
  }

  /**
   * Sends a query that presumably creates or updates something.
   *
   * @param query A cypher statement, create or update.
   * @param params Statement parameters.
   * @param commit A boolean, if set, which commits the transaction.
   * @param txOpt An option containing the transaction in which
   *              this query should be executed.
   * @return a Future of Unit.
   */
  def execute(query: String, params: Map[String, Any] = Map(), commit: Boolean = true)
             (implicit txOpt: Option[Transaction] = None): Future[Unit] = {
    cypher(query, params, commit)(txOpt) {
      case ok: Ok        => Future.Unit
    }
  }

  /**
   * Resets transaction timeout
   * @param tx Transaction identifier
   * @return A Future of Unit that confirms the process
   *         was successful.
   */
  def resetTxTimeout(tx: Transaction): Future[Unit] =
    doTxEndpointRequest(ResetTxTimeout(tx)) {
      case _: Ok => Future.Unit
    }

  /**
   * Helper function for converting errors into server exceptions.
   */
  private[neo4j] def doTxEndpointRequest[T](request: Request)(handler: PartialFunction[Result, Future[T]]) =
    doRequest(request) {
      handler orElse {
        case e: Error => Future.exception(new ServerError(e.code, e.message))
      }
    }

}
