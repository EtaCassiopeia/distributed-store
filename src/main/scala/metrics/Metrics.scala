package metrics

import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.{Executors, TimeUnit}
import javax.management.ObjectName

import com.codahale.metrics.{JmxReporter, MetricRegistry}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import common.{IdGenerator, Reference}
import org.elasticsearch.metrics.ElasticsearchReporter
import org.joda.time.DateTime
import play.api.libs.json.{JsArray, Json}

import scala.collection.JavaConversions._
import scala.util.Try

class Metrics {

  private[this] val metrics = new MetricRegistry
  private[this] val commandsTimerClient = metrics.timer("operations.client")
  private[this] val commandsAwaitTimer = metrics.timer("operations.await")
  private[this] val commandsTimerOut = metrics.timer("operations.out")
  private[this] val commandsTimerIn = metrics.timer("operations.in")
  private[this] val rollbackMeter = metrics.timer("operations.rollback")
  private[this] val locksMeter = metrics.meter("operations.locked")
  private[this] val locksRetryMeter = metrics.meter("operations.locked.retry")
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

  private[this] val jmxReporter = Reference.empty[JmxReporter]()
  private[this] val esReporter = Reference.empty[ElasticsearchReporter]()

  def endAwait(start: Long) = commandsAwaitTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS)
  def startQuorum = quorumTimer.time()
  def startQuorumAggr = quorumAggregateTimer.time()
  def startCommandclient = commandsTimerClient.time()
  def startCommand = commandsTimerOut.time()
  def startCommandIn = commandsTimerIn.time()
  def quorumSuccess = quorumSuccessMeter.mark()
  def quorumFailure = quorumFailureMeter.mark()
  def quorumRetryFailure = quorumFailureRetryMeter.mark()
  def rollback = rollbackMeter.time()
  def lock = locksMeter.mark()
  def lockRetry = locksRetryMeter.mark()
  def read = readsMeter.time()
  def write = writesMeter.time()
  def delete = deleteMeter.time()
  def cacheSync = cacheSyncMeter.time()
  def balance = balanceMeter.time()
  def balanceKeys(n: Int) = balanceKeysMeter.mark(n)

  private[this] val server = Reference.empty[HttpServer]()

  private[this] lazy val mbs = ManagementFactory.getPlatformMBeanServer

  private[this] lazy val gen = IdGenerator(512)

  private[this] def dataFromJMX(name: String): JsArray = {
    var obj = Json.arr()
    Try {
      for (objectname <- mbs.queryNames(new ObjectName(s"$name:name=*"), null).toList.sortWith { (o1, o2) => o1.getCanonicalName.compareTo(o2.getCanonicalName) < 0 }) {
        var bean = Json.obj("name" -> objectname.getCanonicalName.replace(s"$name:name=", "").replace(".", "-"))
        bean = bean ++ Json.obj("_id" -> s"${gen.nextId()}")
        bean = bean ++ Json.obj("@timestamp" -> DateTime.now().getMillis)//.toString("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        mbs.getMBeanInfo(objectname).getAttributes.map { info =>
          mbs.getAttribute(objectname, info.getName) match {
            case a if a.getClass == classOf[String] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[String])
            case a if a.getClass == classOf[java.lang.Long] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Long])
            case a if a.getClass == classOf[java.lang.Double] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Double])
            case a if a.getClass == classOf[java.lang.Integer] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Int])
            case a if a.getClass == classOf[java.lang.Boolean] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Boolean])
            case a =>
          }
        }
        obj = obj :+ bean
      }
    }
    obj
  }

  def start(name: String = "distributed-map", port: Int = 9999) = {

    jmxReporter <== JmxReporter.forRegistry(metrics).inDomain(name).build()
    jmxReporter().start()

    //esReporter <== ElasticsearchReporter.forRegistry(metrics)
    //  .hosts("localhost:9200")
    //  .index("blah")
    //  .build()
    //
    //esReporter().start(1, TimeUnit.SECONDS)

    server <== HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0)
    server().setExecutor(Executors.newFixedThreadPool(1))
    server().createContext("/metrics.json", new HttpHandler {
      override def handle(p1: HttpExchange): Unit = {
        val data = Json.stringify(dataFromJMX(name)).getBytes(Charset.forName("UTF-8"))
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
    jmxReporter().stop()
  }
}

