import java.net.InetAddress
import java.util.concurrent.{TimeUnit, Executors}

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import common.IdGenerator
import play.api.libs.json.Json
import server.{NodeClient, ClusterEnv, KeyValNode}

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.util.{Try, Failure, Success}

object EC2Node extends App {

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(40))

  val remotePort = Try { args(2).toInt }.toOption.getOrElse(7000)
  val remoteHost = Try { args(1) }.toOption.getOrElse(InetAddress.getLocalHost.getHostAddress)
  val httpPort = Try { args(3).toInt }.toOption.getOrElse(9999)
  val nodeName = Try { args(0) }.toOption.getOrElse("dbnode")

  val env = ClusterEnv(3)
  val node1 = KeyValNode(nodeName, env)

  env.start(name = s"$nodeName-distributed-map", port = httpPort)
  node1.start(remoteHost, remotePort)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      env.stop()
      node1.stop().destroy()
    }
  })
}

object EC2SlaveNode extends App {

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(40))

  val remoteHost = Try { args(1) }.toOption.getOrElse("172.31.19.83:7000")
  val httpPort = Try { args(2).toInt }.toOption.getOrElse(9999)
  val nodeName = Try { args(0) }.toOption.getOrElse("dbnode")

  val env = ClusterEnv(3)
  val node1 = KeyValNode(nodeName, env)

  env.start(name = s"$nodeName-distributed-map", port = httpPort)
  node1.start(seedNodes = Seq(remoteHost))

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      env.stop()
      node1.stop().destroy()
    }
  })
}

object EC2Client extends App {

  val remoteHost = Try { args(0) }.toOption.getOrElse("172.31.19.83")
  val remotePort = Try { args(1).toInt  }.toOption.getOrElse(7000)
  val clientNbr = Try { args(2).toInt }.toOption.getOrElse(50)

  val timeout = Duration(120, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(40))
  val userEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(clientNbr))
  val metrics = new MetricRegistry
  val done = metrics.meter("operations.done")
  val success = metrics.meter("operations.success")
  val failW = metrics.meter("operations.write.fail")
  val failD = metrics.meter("operations.delete.fail")
  val fail1 = metrics.meter("operations.read.fail1")
  val fail2 = metrics.meter("operations.read.fail2")
  val fail3 = metrics.meter("operations.read.fail3")

  val env = ClusterEnv(3)
  val client = NodeClient(env)
  client.start(seedNodes = Seq(s"$remoteHost:$remotePort"))
  ConsoleReporter.forRegistry(metrics).build().start(30, TimeUnit.SECONDS)

  Thread.sleep(30000)

  def scenario: Unit = {
    for (i <- 0 to 100) {
      var seq = Seq[String]()
      for (j <- 0 to 100) {
        val id = IdGenerator.uuid
        seq = seq :+ id
        Await.result(client.set(id)(Json.obj("hello" -> "world", "id" -> id, "stuff1" -> IdGenerator.extendedToken(256), "stuff2" -> IdGenerator.extendedToken(256))).andThen {
          case Success(_) => success.mark()
          case Failure(_) => failW.mark()
        }, timeout)
        done.mark()
      }
      seq.foreach { id =>
        Await.result(client.get(id).andThen {
          case scala.util.Success(Some(value)) if value.\("id").as[String] == id => success.mark()
          case scala.util.Success(Some(value)) if value.\("id").as[String] != id => fail1.mark()
          case scala.util.Success(None) => fail2.mark()
          case scala.util.Failure(e) => fail3.mark()
        }.recover {
          case _ => Future.successful(())
        }, timeout)
        done.mark()
      }
      seq.foreach { id =>
        Await.result(client.delete(id).andThen {
          case Success(_) => success.mark()
          case Failure(_) => failD.mark()
        }, timeout)
        done.mark()
      }
    }
  }
  var list = List[Future[Unit]]()
  for (i <- 0 to clientNbr) {
    list = list :+ Future(scenario)(userEc)
  }
  val fu = Future.sequence(list)
  Await.result(fu, Duration(36000, TimeUnit.SECONDS))

  env.stop()
  client.stop()

}
