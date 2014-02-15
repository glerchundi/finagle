package com.twitter.finagle.neo4j.protocol.endpoints.transaction

import com.twitter.finagle.neo4j.protocol.{Result, Transaction}
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.neo4j.util.MapperUtils
import scala.collection.{LinearSeq, SeqLike}

case class Row(values: IndexedSeq[Any])

object TransactionResults {

  private case class JsonResponse(commit: Option[String],
                                  results: Seq[JsonResult],
                                  transaction: Option[JsonTransaction],
                                  errors: Seq[Error])
  private case class JsonResult(columns: Seq[String], data: Seq[JsonData])
  private case class JsonTransaction(expires: String)
  private case class JsonError(code: String, message: String)
  private case class JsonData(row: Seq[Any])

  private def decodeTx(commitOpt: Option[String], txOpt: Option[JsonTransaction]): Option[Transaction] =
    commitOpt map { c => Transaction(c, txOpt.map { tx => tx.expires } getOrElse "") }

  def decode(httpResponse: HttpResponse): Result = {
    val content = httpResponse.getContent.toString(CharsetUtil.UTF_8)
    val response = MapperUtils.readJson[JsonResponse](content).get()
    response match {
      case JsonResponse(_, _, _, e) if !e.isEmpty => Error(e.head.code, e.head.message)
      case JsonResponse(c, Seq(), t, _) => Ok(decodeTx(c, t))
      case JsonResponse(c, r, t, _) =>
        ResultSet(
          decodeTx(c, t),
          r.head.columns,
          r.head.data map (d => Row(d.row.toIndexedSeq))
        )
    }
  }

}

case class Ok(txOpt: Option[Transaction])
  extends Result

case class ResultSet(txOpt: Option[Transaction],
                     columns: Seq[String],
                     rows: Seq[Row])
  extends Result

case class Error(code: String, message: String)
  extends Result {
  override val isError = true
}
