package com.twitter.finagle.neo4j.integration

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.Neo4j
import com.twitter.finagle.neo4j.protocol.{Neo4j => Neo4jCodec}
import com.twitter.util.Await
import org.jboss.netty.handler.codec.http.HttpMethod


class ClientServerIntegrationSpec extends SpecificationWithJUnit {

  lazy val client = Neo4j.newRichClient("127.0.0.1:7474")

  "A client" should {

    "set & get" in {

      val f = for {
        (txOpt, rs) <- client.select("MATCH (n) RETURN COUNT(n), n") {
          row => row
        }
      } yield rs
      Await.result(f)
      true must beTrue
    }

  }
}
