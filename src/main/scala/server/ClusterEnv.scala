package server

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.{Executors, TimeUnit}

import com.codahale.metrics.{JmxReporter, ConsoleReporter, MetricRegistry}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import common.Reference
import config.Env
import jmx.JMXMonitor
import play.api.libs.json.Json

case class ClusterEnv(replicates: Int) {

  private[this] val metrics = new MetricRegistry
  private[this] val commandsTimerOut = metrics.timer("operations.out")
  private[this] val commandsTimerIn = metrics.timer("operations.in")
  private[this] val readsMeter = metrics.meter("operations.reads")
  private[this] val writesMeter = metrics.meter("operations.writes")
  private[this] val deleteMeter = metrics.meter("operations.deletes")
  private[this] val cacheSyncMeter = metrics.meter("cache.sync")
  private[this] val balanceMeter = metrics.meter("balance.sync")
  private[this] val balanceKeysMeter = metrics.meter("balance.keys")
  private[this] val quorumFailureRetryMeter = metrics.meter("quorum.failures.with.retry")
  private[this] val quorumFailureMeter = metrics.meter("quorum.failures")
  private[this] val quorumSuccessMeter = metrics.meter("quorum.success")
  //private[this] val reporter = ConsoleReporter.forRegistry(metrics).build()
  private[this] val jmxReporter = JmxReporter.forRegistry(metrics).inDomain("distributed-map").build()
  // TODO : rebalance timer
  // TODO : cache timer
  def startCommand = commandsTimerOut.time()
  def startCommandIn = commandsTimerIn.time()
  def quorumSuccess = quorumSuccessMeter.mark()
  def quorumFailure = quorumFailureMeter.mark()
  def quorumRetryFailure = quorumFailureRetryMeter.mark()
  def read = readsMeter.mark()
  def write = writesMeter.mark()
  def delete = deleteMeter.mark()
  def cacheSync = cacheSyncMeter.mark()
  def balance = balanceMeter.mark()
  def balanceKeys(n: Int) = balanceKeysMeter.mark(n)

  private[this] val server = Reference.empty[HttpServer]()

  def start() = {
    jmxReporter.start()

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
    //reporter.start(10, TimeUnit.SECONDS)
  }

  def stop() = {
    server.foreach(_.stop(0))
    jmxReporter.stop()
    //reporter.stop()
  }
}

