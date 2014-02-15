package com.twitter.finagle.neo4j

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import com.twitter.finagle.neo4j.protocol._
import com.twitter.util.Future

object Client {

  /**
   * Construct a client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client = Client(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(Neo4j())
      .daemon(daemonize = true)
      .build())

  /**
   * Construct a client from a single Service.
   */
  def apply(raw: Service[Request, Result]): Client =
    new Client(raw)

}

class Client(service: Service[Request, Result])
  extends BaseClient(service)
  with TransactionEndpoint

/**
 * Connects to a single Neo4j host
 * @param service: Finagle service object built with the Neo4j codec
 */
class BaseClient(service: Service[Request, Result]) {

  /**
   * Releases underlying service object
   * @return A Future of Unit that confirms the process
   *         was successful.
   */
  def release() = service.close()

  /**
   * Helper function for passing a request to the service
   */
  private[neo4j] def doRequest[T](request: Request)(handler: PartialFunction[Result, Future[T]]) =
    service(request) flatMap (handler orElse {
      case _        => Future.exception(new IllegalStateException)
    })

}
