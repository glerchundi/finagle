package com.twitter.finagle.builder

import collection.JavaConversions._

import java.net.{SocketAddress, InetSocketAddress}
import java.util.Collection
import java.util.logging.Logger
import java.util.concurrent.Executors

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.service
import com.twitter.finagle.service.{Service, Filter, RetryingService, TimeoutFilter}
import com.twitter.finagle.stats.{StatsRepository, TimeWindowedStatsRepository, StatsReceiver}
import com.twitter.finagle.loadbalancer.{
  LoadBalancerService, LoadBalancerStrategy,
  LeastLoadedStrategy, FailureAccrualStrategy}

object ClientBuilder {
  def apply() = new ClientBuilder
  def get() = apply()

  val defaultChannelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

  def parseHosts(hosts: String): java.util.List[InetSocketAddress] = {
    val hostPorts = hosts split Array(' ', ',') filter (_ != "") map (_.split(":"))
    hostPorts map { hp => new InetSocketAddress(hp(0), hp(1).toInt) } toList
  }
}

// TODO: sampleGranularity, sampleWindow <- rename!

/**
 * A word about the default values:
 *
 *   o connectionTimeout: optimized for within a datanceter
 *   o by default, no request timeout
 */
case class ClientBuilder(
  _hosts: Option[Seq[SocketAddress]],
  _codec: Option[Codec],
  _connectionTimeout: Duration,
  _requestTimeout: Duration,
  _statsReceiver: Option[StatsReceiver],
  _loadStatistics: (Int, Duration),
  _name: Option[String],
  _hostConnectionLimit: Option[Int],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _retries: Option[Int],
  _logger: Option[Logger],
  _channelFactory: Option[ChannelFactory],
  _proactivelyConnect: Option[Duration])
  // _loadBalancerStrategy: Option[LoadBalancerStrategy])
{
  import ClientBuilder._
  def this() = this(
    None,                // hosts
    None,                // codec
    10.milliseconds,     // connectionTimeout
    Duration.MaxValue,   // requestTimeout
    None,                // statsReceiver
    (60, 10.seconds),    // loadStatistics
    None,                // name
    None,                // hostConnectionLimit
    None,                // sendBufferSize
    None,                // recvBufferSize
    None,                // retries
    None,                // logger
    None,                // channelFactory
    None// ,                // proactivelyConnect
    //None                 // loadBalancerStrategy
  )

  override def toString() = {
    val options = Seq(
      "name"                -> _name,
      "hosts"               -> _hosts,
      "codec"               -> _codec,
      "connectionTimeout"   -> Some(_connectionTimeout),
      "requestTimeout"      -> Some(_requestTimeout),
      "statsReceiver"       -> _statsReceiver,
      "loadStatistics"      -> _loadStatistics,
      "hostConnectionLimit" -> Some(_hostConnectionLimit),
      "sendBufferSize"      -> _sendBufferSize,
      "recvBufferSize"      -> _recvBufferSize,
      "retries"             -> _retries,
      "logger"              -> _logger,
      "channelFactory"      -> _channelFactory,
      "proactivelyConnect"  -> _proactivelyConnect
    )

    "ClientBuilder(%s)".format(
      options flatMap {
        case (k, Some(v)) => Some("%s=%s".format(k, v))
        case _ => None
      } mkString(", "))
  }

  def hosts(hostnamePortCombinations: String): ClientBuilder =
    copy(_hosts = Some(parseHosts(hostnamePortCombinations)))

  def hosts(addresses: Collection[SocketAddress]): ClientBuilder =
    copy(_hosts = Some(addresses toSeq))

  def hosts(addresses: Iterable[SocketAddress]): ClientBuilder =
    copy(_hosts = Some(addresses toSeq))

  def codec(codec: Codec): ClientBuilder =
    copy(_codec = Some(codec))

  def connectionTimeout(duration: Duration): ClientBuilder =
    copy(_connectionTimeout = duration)

  def requestTimeout(duration: Duration): ClientBuilder =
    copy(_requestTimeout = duration)

  def reportTo(receiver: StatsReceiver): ClientBuilder =
    copy(_statsReceiver = Some(receiver))

  /**
   * The interval over which to aggregate load statistics.
   */
  def loadStatistics(numIntervals: Int, interval: Duration): ClientBuilder = {
    require(numIntervals >= 1, "Must have at least 1 window to sample statistics over")

    copy(_loadStatistics = (numIntervals, interval))
  }

  def name(value: String): ClientBuilder = copy(_name = Some(value))

  def hostConnectionLimit(value: Int): ClientBuilder =
    copy(_hostConnectionLimit = Some(value))

  def retries(value: Int): ClientBuilder =
    copy(_retries = Some(value))

  def sendBufferSize(value: Int): ClientBuilder = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int): ClientBuilder = copy(_recvBufferSize = Some(value))

  def channelFactory(cf: ChannelFactory): ClientBuilder =
    copy(_channelFactory = Some(cf))

  def proactivelyConnect(duration: Duration): ClientBuilder =
    copy(_proactivelyConnect = Some(duration))

  // ** BUILDING
  def logger(logger: Logger): ClientBuilder = copy(_logger = Some(logger))

  private def bootstrap(codec: Codec)(host: SocketAddress) = {
    val bs = new BrokerClientBootstrap(_channelFactory getOrElse defaultChannelFactory)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = codec.clientPipelineFactory.getPipeline
        for (logger <- _logger) {
          pipeline.addFirst(
            "channelSnooper",
            ChannelSnooper(_name getOrElse "client")(logger.info))
        }

        pipeline
      }
    }
    bs.setPipelineFactory(pf)
    bs.setOption("remoteAddress", host)
    bs.setOption("connectTimeoutMillis", _connectionTimeout.inMilliseconds)
    bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
    // bs.setOption("soLinger", 0)  (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private def pool(limit: Option[Int], proactivelyConnect: Option[Duration])
                  (bootstrap: BrokerClientBootstrap) =
    limit match {
      case Some(limit) =>
        new ConnectionLimitingChannelPool(bootstrap, limit, proactivelyConnect)
      case None =>
        new ChannelPool(bootstrap, proactivelyConnect)
    }

  private def retryingFilter =
    _retries map { RetryingService.tries[Any, Any](_) }

  def makeBroker(codec: Codec) =
    bootstrap(codec) _                                andThen
    pool(_hostConnectionLimit, _proactivelyConnect) _ andThen
    (new PoolingBroker(_))

  def build(): Service[Any, Any] = {
    if (!_hosts.isDefined || _hosts.get.isEmpty)
      throw new IncompleteSpecification("No hosts were specified")
    if (!_codec.isDefined)
      throw new IncompleteSpecification("No codec was specified")
    
    val hosts = _hosts.get
    val codec = _codec.get

    val brokers = hosts map { host =>
      // TODO: stats export [observers], internal LB stats.
      makeBroker(codec)(host)
    }

    val timedoutBrokers =
      if (_requestTimeout < Duration.MaxValue) {
        val timeoutFilter = new TimeoutFilter[Any, Any](Timer.default, _requestTimeout)
        brokers map { broker => timeoutFilter andThen broker }
      } else {
        brokers
      }

    val loadBalancerStrategy = // _loadBalancerStrategy getOrElse
    {
      val leastLoadedStrategy = new LeastLoadedStrategy[Any, Any]
      new FailureAccrualStrategy(leastLoadedStrategy, 3, 10.seconds)
    }

    val loadBalanced = new LoadBalancerService(timedoutBrokers, loadBalancerStrategy)
    retryingFilter map { filter => filter andThen loadBalanced } getOrElse { loadBalanced }
  }

  def buildService[Request, Reply]() =
    new service.Client[Request, Reply](build())
}