package com.twitter.finagle.neo4j.protocol

import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelHandler}
import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http.HttpResponse
import com.twitter.finagle.neo4j.protocol.endpoints.transaction.TransactionResults

/**
 * Convert Netty responses to a Neo4j results
 */
class ResultDecoder extends SimpleChannelHandler {
  private[this] val log = Logger("finagle-neo4j")

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case httpResponse: HttpResponse =>
        Channels.fireMessageReceived(ctx, TransactionResults.decode(httpResponse))
      case unknown =>
        log.warning("ResultDecoder.messageReceived: illegal message type: %s", unknown.getClass)
        Channels.disconnect(ctx.getChannel)
    }
  }

}
