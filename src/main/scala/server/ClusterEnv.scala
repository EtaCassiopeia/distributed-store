package server

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.{Future, Executors, TimeUnit}

import com.codahale.metrics.{JmxReporter, ConsoleReporter, MetricRegistry}
import com.librato.metrics.HttpPoster.Response
import com.librato.metrics.{NingHttpPoster, HttpPoster, LibratoReporter}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import common.{IdGenerator, Reference}
import config.Env
import jmx.JMXMonitor
import play.api.libs.json.Json

case class ClusterEnv(replicates: Int) {

  private[this] val metrics = new MetricRegistry
  private[this] val commandsTimerClient = metrics.timer("operations.client")
  private[this] val commandsTimerOut = metrics.timer("operations.out")
  private[this] val commandsTimerIn = metrics.timer("operations.in")
  private[this] val rollbackMeter = metrics.timer("operations.rollback")
  private[this] val readsMeter = metrics.timer("operations.reads")
  private[this] val writesMeter = metrics.timer("operations.writes")
  private[this] val deleteMeter = metrics.timer("operations.deletes")
  private[this] val cacheSyncMeter = metrics.timer("cache.sync")
  private[this] val balanceMeter = metrics.timer("balance.sync")
  private[this] val balanceKeysMeter = metrics.meter("balance.keys")
  private[this] val quorumFailureRetryMeter = metrics.meter("quorum.failures.with.retry")
  private[this] val quorumFailureMeter = metrics.meter("quorum.failures")
  private[this] val quorumSuccessMeter = metrics.meter("quorum.success")
  private[this] val quorumTimer = metrics.timer("quorum.time")
  private[this] val quorumAggregateTimer = metrics.timer("quorum.aggregate")

  private[this] val jmxReporter = JmxReporter.forRegistry(metrics).inDomain("distributed-map").build()

  def startQuorum = quorumTimer.time()
  def startQuorumAggr = quorumAggregateTimer.time()
  def startCommandclient = commandsTimerClient.time()
  def startCommand = commandsTimerOut.time()
  def startCommandIn = commandsTimerIn.time()
  def quorumSuccess = quorumSuccessMeter.mark()
  def quorumFailure = quorumFailureMeter.mark()
  def quorumRetryFailure = quorumFailureRetryMeter.mark()
  def rollback = rollbackMeter.time()
  def read = readsMeter.time()
  def write = writesMeter.time()
  def delete = deleteMeter.time()
  def cacheSync = cacheSyncMeter.time()
  def balance = balanceMeter.time()
  def balanceKeys(n: Int) = balanceKeysMeter.mark(n)

  private[this] val server = Reference.empty[HttpServer]()

  def start() = {
    jmxReporter.start()

    // val username = "mathieu.ancelin@gmail.com"
    // val token = IdGenerator.token
    // LibratoReporter.builder(metrics, username, token, "distributed-map")
    //   .setHttpPoster(NingHttpPoster.newPoster(username, token, "http://localhost:9000"))
    //   .build().start(10, TimeUnit.SECONDS)

    server <== HttpServer.create(new InetSocketAddress("0.0.0.0", 9999), 0)
    server().setExecutor(Executors.newFixedThreadPool(1))
    server().createContext("/metrics.json", new HttpHandler {
      override def handle(p1: HttpExchange): Unit = {
        val data = Json.stringify(JMXMonitor.data()).getBytes(Charset.forName("UTF-8"))
        p1.getResponseHeaders.add("Content-Type", "application/json")
        p1.getResponseHeaders.add("Content-Length", data.length + "")
        p1.getResponseHeaders.add("Access-Control-Allow-Origin", "*")
        p1.sendResponseHeaders(200, data.length)
        p1.getResponseBody.write(data)
        p1.close()
      }
    })
    server().start()
  }

  def stop() = {
    server.foreach(_.stop(0))
    jmxReporter.stop()
  }
}

