package com.twitter.finagle.neo4j.protocol

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpMethod, HttpRequest}
import com.twitter.finagle.http.{MediaType, Message}
import com.twitter.finagle.http.netty.HttpRequestProxy
import org.jboss.netty.handler.codec.http.HttpVersion._

abstract class Request extends Message with HttpRequestProxy {
  def isRequest = true
}

class SimpleRequest(path: String,
                    method: HttpMethod,
                    body: String)
  extends Request {
  val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, method, "/db/data" + path)

  this.contentType   = Message.ContentTypeJson
  this.accept        = MediaType.Json
  this.contentString = body
}
