import java.util.concurrent.{Executors, TimeUnit}

import common.IdGenerator
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server.{ClusterEnv, DistributedMapNode, NodeClient, OpStatus}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class Load2Spec extends Specification with Tags {
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

    val env = ClusterEnv(2)
    val node1 = DistributedMapNode(s"node1-${IdGenerator.uuid}", env)
    val node2 = DistributedMapNode(s"node2-${IdGenerator.uuid}", env)
    val node3 = DistributedMapNode(s"node3-${IdGenerator.uuid}", env)
    val node4 = DistributedMapNode(s"node4-${IdGenerator.uuid}", env)
    val node5 = DistributedMapNode(s"node5-${IdGenerator.uuid}", env)
    val client = NodeClient(env)

    "Start some nodes" in {
      node1.start()
      node2.start()
      node3.start()
      node4.start()
      node5.start()
      client.start()
      Thread.sleep(3000)   // Wait for cluster setup
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
      client.stop()
      success
    }
  }
}