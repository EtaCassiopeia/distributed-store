import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import common.IdGenerator
import config.ClusterEnv
import play.api.libs.json.{JsValue, Json}
import server.{KeyValNode, NodeClient}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import server.implicits._

object Constants {
  val host = "192.168.1.11"//"127.0.0.1" //"192.168.1.34"
  val port = 7000
  val both = s"$host:$port"
}

object Host1WithClient extends App {

  val nbrClients = 100
  val nbrNodes = 4
  val nbrReplicates = 3
  implicit val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val userEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nbrClients))

  val env = ClusterEnv(2)
  val node1 = KeyValNode(s"node1", env)
  val node2 = KeyValNode(s"node2", env)
  val node3 = KeyValNode(s"node3", env)
  val client = NodeClient(env)
  val counter = new AtomicInteger(0)
  val successes = new AtomicInteger(0)
  val failuresR1 = new AtomicInteger(0)
  val failuresR2= new AtomicInteger(0)
  val failuresR3 = new AtomicInteger(0)
  val failuresW = new AtomicInteger(0)
  val failuresD = new AtomicInteger(0)

  def incr = if (counter.incrementAndGet() % 20000 == 0) println(s"Operations : ${counter.get()} / 3000000")
  def success = successes.incrementAndGet()//if (successes.incrementAndGet() % 10000 == 0) println(s"Successes : ${successes.get()} / 3000000")
  def failR1 = if (failuresR1.incrementAndGet() % 10 == 0) println(s"Failures Read not good data: ${failuresR1.get()}")
  def failR2 = if (failuresR2.incrementAndGet() % 10 == 0) println(s"Failures Read nothing : ${failuresR2.get()}")
  def failR3 = if (failuresR3.incrementAndGet() % 10 == 0) println(s"Failures Read fail : ${failuresR3.get()}")
  def failW = if (failuresW.incrementAndGet() % 10 == 0) println(s"Failures Write : ${failuresW.get()}")
  def failD = if (failuresD.incrementAndGet() % 10 == 0) println(s"Failures Delete : ${failuresD.get()}")

  env.start()
  node1.start(Constants.host, Constants.port)
  node2.start(seedNodes = Seq(Constants.both))
  node3.start(seedNodes = Seq(Constants.both))
  client.start(seedNodes = Seq(Constants.both))

  Thread.sleep(20000)

  def scenario: Unit = {
    for (i <- 0 to 100) {
      var seq = Seq[String]()
      for (j <- 0 to 100) {
        val id = IdGenerator.uuid
        seq = seq :+ id
        Await.result(client.set[JsValue](id, Json.obj("hello" -> "world", "id" -> id, "stuff1" -> IdGenerator.extendedToken(256), "stuff2" -> IdGenerator.extendedToken(256))).andThen {
          case Success(_) => success
          case Failure(_) => failW
        }, timeout)
        incr
      }
      seq.foreach { id =>
        Await.result(client.get(id).andThen {
          case scala.util.Success(Some(value)) if value.\("id").as[String] == id => success
          case scala.util.Success(Some(value)) if value.\("id").as[String] != id => failR1
          case scala.util.Success(None) => failR2
          case scala.util.Failure(e) => failR3
        }.recover {
          case _ => Future.successful(())
        }, timeout)
        incr
      }
      seq.foreach { id =>
        Await.result(client.delete(id).andThen {
          case Success(_) => success
          case Failure(_) => failD
        }, timeout)
        incr
      }
    }
  }
  var list = List[Future[Unit]]()
  for (i <- 0 to nbrClients) {
    list = list :+ Future(scenario)(userEc)
  }
  val fu = Future.sequence(list)
  Await.result(fu, Duration(3600, TimeUnit.SECONDS))

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      env.stop()
      client.stop()
      node1.stop().destroy()
      node2.stop().destroy()
      node3.stop().destroy()
    }
  })

}

object Host2 extends App {

  implicit val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  val env = ClusterEnv(3)
  val node1 = KeyValNode(s"node1", env)
  val node2 = KeyValNode(s"node2", env)
  val node3 = KeyValNode(s"node3", env)

  env.start()
  node1.start(seedNodes = Seq(Constants.both))
  node2.start(seedNodes = Seq(Constants.both))
  node3.start(seedNodes = Seq(Constants.both))

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      env.stop()
      node1.stop().destroy()
      node2.stop().destroy()
      node3.stop().destroy()
    }
  })
}

object SimpleHost extends App {

  implicit val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  val env = ClusterEnv(0)
  val node1 = KeyValNode(s"node-${IdGenerator.token(6)}", env)

  env.start()
  node1.start()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      env.stop()
      node1.stop().destroy()
    }
  })
}

object SimpleHostWithClients extends App {

  implicit val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val userEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))

  val env = ClusterEnv(0)
  val node1 = KeyValNode(s"node-${IdGenerator.token(6)}", env)
  val client = NodeClient(env)

  env.start()
  node1.start("127.0.0.1", 7000)
  client.start(seedNodes = Seq("127.0.0.1:7000"))

  Thread.sleep(5000)

  def scenario: Unit = {
    for (i <- 0 to 100) {
      var seq = Seq[String]()
      for (j <- 0 to 1000) {
        val id = IdGenerator.uuid
        seq = seq :+ id
        Await.result(client.set[JsValue](id, Json.obj("hello" -> "world", "id" -> id, "stuff1" -> IdGenerator.extendedToken(256), "stuff2" -> IdGenerator.extendedToken(256))), timeout)
      }
      seq.foreach { id =>
        Await.result(client.get(id), timeout)
      }
      seq.foreach { id =>
        Await.result(client.delete(id), timeout)
      }
    }
  }
  //Await.result(Future.sequence(Seq(Future(scenario)(userEc), Future(scenario)(userEc), Future(scenario)(userEc))), Duration(3600, TimeUnit.SECONDS))
  Await.result(Future.sequence(Seq(Future(scenario)(userEc))), Duration(3600, TimeUnit.SECONDS))

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      env.stop()
      node1.stop().destroy()
    }
  })

}