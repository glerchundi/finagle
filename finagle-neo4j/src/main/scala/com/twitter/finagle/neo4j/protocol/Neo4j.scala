package com.twitter.finagle.neo4j.protocol

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle._
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.ServerCodecConfig
import com.twitter.finagle.ClientCodecConfig
import org.jboss.netty.handler.codec.http.{HttpContentDecompressor, HttpChunkAggregator, HttpClientCodec}
import com.twitter.conversions.storage._

object Neo4jClientPipelineFactory extends ChannelPipelineFactory {

  def getPipeline = {
    // Taken from Http codec (15/02/2014)
    val pipeline = Channels.pipeline()
    pipeline.addLast("httpCodec", new HttpClientCodec())
    pipeline.addLast("httpDechunker", new HttpChunkAggregator(5.megabytes.inBytes.toInt))
    pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

    // Neo4j encoder and decoders
    pipeline.addLast("requestDecoder", new RequestEncoder)
    pipeline.addLast("responseEncoder", new ResultDecoder)
    pipeline
  }
}

object Neo4jTransporter extends Netty3Transporter[Request, Result]("neo4j", Neo4jClientPipelineFactory)

object Neo4j {

  def apply(stats: StatsReceiver = NullStatsReceiver) = new Neo4j(stats)
  def get() = apply()
}

class Neo4j(stats: StatsReceiver) extends CodecFactory[Request, Result] {

  def this() = this(NullStatsReceiver)

  def client: ClientCodecConfig => Codec[Request, Result] =
    Function.const {
      new Codec[Request, Result] {
        def pipelineFactory = Neo4jClientPipelineFactory

        override def prepareConnFactory(underlying: ServiceFactory[Request, Result]) = {
          new Neo4jTracingFilter() andThen new Neo4jLoggingFilter(stats) andThen underlying
        }
      }
    }

  def server: ServerCodecConfig => Codec[Request, Result] =
    throw new UnsupportedOperationException("This is a client side only codec factory")
}

private class Neo4jTracingFilter extends SimpleFilter[Request, Result] {

  override def apply(request: Request, service: Service[Request, Result]) = Trace.unwind {
    if (Trace.isActivelyTracing) {
      // TODO: use something decent instead of .getClass.toString ...
      Trace.recordRpcname("neo4j", request.getClass.toString)
      Trace.record(Annotation.ClientSend())
      service(request) map { result =>
        Trace.record(Annotation.ClientRecv())
        result
      }
    }
    else
      service(request)
  }
}

private class Neo4jLoggingFilter(stats: StatsReceiver)
  extends SimpleFilter[Request, Result] {

  private[this] val error = stats.scope("error")
  private[this] val succ  = stats.scope("success")

  override def apply(request: Request, service: Service[Request, Result]) = {
    service(request) map { result =>
      // TODO: use something decent instead of .getClass.toString ...
      val name = request.getClass.toString
      result match {
        case x if !x.isError => succ.counter(name).incr()
        case _               => error.counter(name).incr()
      }
      result
    }
  }
}