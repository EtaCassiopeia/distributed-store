import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import common.IdGenerator
import config.ClusterEnv
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import server.implicits._

class LoadSpec extends Specification with Tags {
  sequential

  val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def performBy(many: Int)(times: Int)(f: => Future[OpStatus]): Unit = {
    val start = System.currentTimeMillis()
    val future = Future.sequence(
      (0 to many).toList.map { _ =>
        Future {
          (0 to times).toList.map { _ =>
            Await.result(f, timeout)
          }
        }
      }
    )
    Await.result(future, Duration(10, TimeUnit.MINUTES))
    println(s"Injection in ${System.currentTimeMillis() - start} ms.")
  }

  "Distributed Map" should {

    val env = ClusterEnv(5)
    val node1 = KeyValNode(s"node1-${IdGenerator.token(6)}", env)
    val node2 = KeyValNode(s"node2-${IdGenerator.token(6)}", env)
    val node3 = KeyValNode(s"node3-${IdGenerator.token(6)}", env)
    val node4 = KeyValNode(s"node4-${IdGenerator.token(6)}", env)
    val node5 = KeyValNode(s"node5-${IdGenerator.token(6)}", env)
    val node6 = KeyValNode(s"node6-${IdGenerator.token(6)}", env)
    val node7 = KeyValNode(s"node7-${IdGenerator.token(6)}", env)
    val node8 = KeyValNode(s"node8-${IdGenerator.token(6)}", env)
    val node9 = KeyValNode(s"node9-${IdGenerator.token(6)}", env)
    val client = NodeClient(env)

    "Start some nodes" in {
      node1.start("127.0.0.1", 7000)
      node2.start(seedNodes = Seq("127.0.0.1:7000"))
      node3.start(seedNodes = Seq("127.0.0.1:7000"))
      node4.start(seedNodes = Seq("127.0.0.1:7000"))
      node5.start(seedNodes = Seq("127.0.0.1:7000"))
      node6.start(seedNodes = Seq("127.0.0.1:7000"))
      node7.start(seedNodes = Seq("127.0.0.1:7000"))
      node8.start(seedNodes = Seq("127.0.0.1:7000"))
      node9.start(seedNodes = Seq("127.0.0.1:7000"))
      client.start(seedNodes = Seq("127.0.0.1:7000"))
      env.start()
      Thread.sleep(6000)   // Wait for cluster setup
      success
    }

    "Insert some stuff" in {
      performBy(100)(1000) {
        val id = IdGenerator.uuid
        client.set(id, Json.obj(
          "Hello" -> "World", "key" -> id
        ))
      }
      success
    }

    "Stop the nodes" in {
      node1.displayStats().stop().destroy()
      node2.displayStats().stop().destroy()
      node3.displayStats().stop().destroy()
      node4.displayStats().stop().destroy()
      node5.displayStats().stop().destroy()
      node6.displayStats().stop().destroy()
      node7.displayStats().stop().destroy()
      node8.displayStats().stop().destroy()
      node9.displayStats().stop().destroy()
      client.stop()
      env.stop()
      success
    }
  }
}

class Load2Spec extends Specification with Tags {
  sequential

  val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def performBy(title: String)(many: Int)(times: Int)(f: => Future[_]): Unit = {
    val start = System.currentTimeMillis()
    val future = Future.sequence(
      (0 to many).toList.map { _ =>
        Future {
          (0 to times).toList.map { _ =>
            Await.result(f, timeout)
          }
        }
      }
    )
    Await.result(future, Duration(10, TimeUnit.MINUTES))
    println(s"\n\n$title in ${System.currentTimeMillis() - start} ms.\n\n")
  }

  "Distributed Map" should {

    val env = ClusterEnv(3, 1, 1)
    val node1 = KeyValNode(s"node1-${IdGenerator.token(6)}", env)
    val node2 = KeyValNode(s"node2-${IdGenerator.token(6)}", env)
    val node3 = KeyValNode(s"node3-${IdGenerator.token(6)}", env)
    val node4 = KeyValNode(s"node4-${IdGenerator.token(6)}", env)
    val node5 = KeyValNode(s"node5-${IdGenerator.token(6)}", env)
    val client = NodeClient(env)

    "Start some nodes" in {
      node1.start("127.0.0.1", 7000)
      node2.start(seedNodes = Seq("127.0.0.1:7000"))
      node3.start(seedNodes = Seq("127.0.0.1:7000"))
      node4.start(seedNodes = Seq("127.0.0.1:7000"))
      node5.start(seedNodes = Seq("127.0.0.1:7000"))
      client.start(seedNodes = Seq("127.0.0.1:7000"))
      env.start()
      Thread.sleep(10000)   // Wait for cluster setup
      success
    }

    "Insert some stuff" in {
      val id = new AtomicInteger(0)
      performBy("Injection")(10)(10000) {
        client.set(id.incrementAndGet().toString, Json.obj(
          "Hello" -> "World", "key" -> id.get()
        ))
      }
      node1.displayStats()
      node2.displayStats()
      node3.displayStats()
      node4.displayStats()
      node5.displayStats()
      println("\n\n")
      success
    }

    "Read some stuff" in {
      val id = new AtomicInteger(0)
      performBy("Read")(100)(1000) {
        client.get(id.incrementAndGet().toString)
      }
      success
    }

    "Stop the nodes" in {
      node1.displayStats().stop().destroy()
      node2.displayStats().stop().destroy()
      node3.displayStats().stop().destroy()
      node4.displayStats().stop().destroy()
      node5.displayStats().stop().destroy()
      client.stop()
      env.stop()
      success
    }
  }
}

class Load3Spec extends Specification with Tags {
  sequential

  val nodeAmount = 3
  val replication = 3
  val clients = 3
  val nbrOps = 100000

  val timeout = Duration(10, TimeUnit.SECONDS)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(400))

  def performBy(title: String, e: ClusterEnv)(many: Int)(times: Int)(f: NodeClient => Future[_]): Unit = {
    val start = System.currentTimeMillis()
    val future = Future.sequence(
      (0 to many).toList.map { _ =>
        val client = NodeClient(e).start(seedNodes = Seq("127.0.0.1:7000"))
        Thread.sleep(3000)
        Future {
          println(s"Now performing $title $times times ..................................")
          (0 to times).toList.map { _ =>
            Await.result(f(client), timeout)
          }
          println(s"Performing $title $times times done !!!!!!!!!!!!!!!!")
        }.andThen { case _ => client.stop() }
      }
    )
    Await.result(future, Duration(10, TimeUnit.MINUTES))
    println(s"\n\n$title in ${System.currentTimeMillis() - start} ms.\n\n")
  }

  "Distributed Map" should {

    val env = ClusterEnv(replication, 1, 1)
    val nodes = for (i <- 0 to nodeAmount - 1) yield KeyValNode(s"node$i-${IdGenerator.token(6)}", env)

    "Start some nodes" in {
      nodes.head.start("127.0.0.1", 7000)
      nodes.tail.map(_.start(seedNodes = Seq("127.0.0.1:7000")))
      env.start()
      Thread.sleep(10000)   // Wait for cluster setup
      success
    }

    "Perform scenario" in {
      val id = new AtomicInteger(0)
      performBy("Scenario", env)(clients)(nbrOps) { client =>
        client.set(id.incrementAndGet().toString, Json.obj(
          "Hello" -> "World", "key" -> id.get()
        ))
        client.get(id.get.toString)
      }
      success
    }

    "Stop the nodes" in {
      nodes.map(_.stop())
      env.stop()
      success
    }
  }
}