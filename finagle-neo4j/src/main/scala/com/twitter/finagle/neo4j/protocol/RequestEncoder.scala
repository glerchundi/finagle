package com.twitter.finagle.neo4j.protocol

import org.jboss.netty.channel.{MessageEvent, ChannelHandlerContext, SimpleChannelDownstreamHandler}
import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http.HttpHeaders

/**
 * Convert Neo4j requests to Netty Requests
 */
class RequestEncoder extends SimpleChannelDownstreamHandler {
  private[this] val log = Logger("finagle-neo4j")

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case request: Request =>
       assert(!request.isChunked)
        if (!request.headers().contains(HttpHeaders.Names.CONTENT_LENGTH))
          request.contentLength = request.getContent().readableBytes
        if (!request.headers().contains(HttpHeaders.Names.HOST))
          request.host = ctx.getChannel.getRemoteAddress.toString
        super.writeRequested(ctx, e)
      case unknown =>
        log.warning("RequestEncoder: illegal message type: %s", unknown)
        super.writeRequested(ctx, e)
    }
  }
}