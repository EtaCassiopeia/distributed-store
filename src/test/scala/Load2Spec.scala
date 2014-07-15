import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import common.IdGenerator
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

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

    val env = ClusterEnv(3)
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
      success
    }
  }
}