package com.twitter.finagle

import com.twitter.finagle.neo4j.protocol.{Neo4jTransporter, Request, Result}
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.pool.ReusingPool

trait Neo4jRichClient { self: Client[Request, Result] =>

  def newRichClient(dest: String): neo4j.Client =
    neo4j.Client(newService(dest))

  def newRichClient(dest: Name, label: String): neo4j.Client =
    neo4j.Client(newService(dest, label))
}

case class Neo4jClient private[finagle]() extends Client[Request, Result]
  with Neo4jRichClient {

  val defaultClient = new DefaultClient[Request, Result](
    name = "neo4j",
    endpointer = Bridge[Request, Result, Request, Result](Neo4jTransporter, new PipeliningDispatcher(_)),
    pool = (sr: StatsReceiver) => new ReusingPool(_, sr)
  )

  override def newClient(dest: Name, label: String): ServiceFactory[Request, Result] =
    defaultClient.newClient(dest, label)
}

object Neo4j extends Neo4jClient()

