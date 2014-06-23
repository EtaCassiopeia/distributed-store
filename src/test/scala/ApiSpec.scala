import java.util.concurrent.{Executors, TimeUnit}

import common.IdGenerator
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server.{DistributedMapNode, NodeClient}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

package object server {
  object TargetAccessor {
    def targets(node: DistributedMapNode, key: String) = {
      node.targets(key)
    }
  }
}

class ApiSpec extends Specification with Tags {
  sequential

  import server.TargetAccessor

  "Distributed Map" should {

    implicit val timeout = Duration(1, TimeUnit.SECONDS)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    val node1 = DistributedMapNode(s"node1-${IdGenerator.uuid}", 4)
    val node2 = DistributedMapNode(s"node2-${IdGenerator.uuid}", 4)
    val node3 = DistributedMapNode(s"node3-${IdGenerator.uuid}", 4)
    val node4 = DistributedMapNode(s"node4-${IdGenerator.uuid}", 4)
    val node5 = DistributedMapNode(s"node5-${IdGenerator.uuid}", 4)
    val node6 = DistributedMapNode(s"node6-${IdGenerator.uuid}", 4)
    val node7 = DistributedMapNode(s"node7-${IdGenerator.uuid}", 4)
    val node8 = DistributedMapNode(s"node8-${IdGenerator.uuid}", 4)
    val node9 = DistributedMapNode(s"node9-${IdGenerator.uuid}", 4)
    val client = NodeClient()
    var keys = Seq[String]()

    "Start some nodes" in {
      node1.start()
      node2.start()
      node3.start()
      node4.start()
      node5.start()
      node6.start()
      node7.start()
      node8.start()
      node9.start()
      client.start()
      Thread.sleep(3000)   // Wait for cluster setup
      success
    }

    "Insert some stuff" in {
      for (i <- 0 to 1000) {
        val id = IdGenerator.uuid
        keys = keys :+ id
        Await.result( client.set(id, Json.obj(
          "Hello" -> "World", "key" -> id
        )), timeout)
      }
      success
    }

    "Read some stuff" in {
      Await.result(client.get("key3"), timeout) should beNone
      keys.foreach { key =>
        Await.result(client.get(key), timeout) shouldEqual Some(Json.obj("Hello" -> "World", "key" -> key))
      }
      success
    }

    "Delete stuff" in {
      keys.foreach { key =>
        Await.result(client.delete(key), timeout)
      }
      success
    }

    "Always target same nodes in the ring" in {
      keys.foreach { key =>
        val targets =  TargetAccessor.targets(node1, key)
        TargetAccessor.targets(node2, key) shouldEqual targets
        TargetAccessor.targets(node2, key).size shouldEqual 5
        TargetAccessor.targets(node3, key) shouldEqual targets
        TargetAccessor.targets(node3, key).size shouldEqual 5
        TargetAccessor.targets(node4, key) shouldEqual targets
        TargetAccessor.targets(node4, key).size shouldEqual 5
        TargetAccessor.targets(node5, key) shouldEqual targets
        TargetAccessor.targets(node5, key).size shouldEqual 5
        TargetAccessor.targets(node6, key) shouldEqual targets
        TargetAccessor.targets(node6, key).size shouldEqual 5
        TargetAccessor.targets(node7, key) shouldEqual targets
        TargetAccessor.targets(node7, key).size shouldEqual 5
        TargetAccessor.targets(node8, key) shouldEqual targets
        TargetAccessor.targets(node8, key).size shouldEqual 5
        TargetAccessor.targets(node9, key) shouldEqual targets
        TargetAccessor.targets(node9, key).size shouldEqual 5
      }
      success
    }

    "Always target same nodes for the same key" in {
      for (i <- 0 to 10) {
        val key = IdGenerator.uuid
        for (j <- 0 to 100) {
          val targets = TargetAccessor.targets(node1, key)
          TargetAccessor.targets(node2, key) shouldEqual targets
          TargetAccessor.targets(node3, key) shouldEqual targets
          TargetAccessor.targets(node4, key) shouldEqual targets
          TargetAccessor.targets(node5, key) shouldEqual targets
          TargetAccessor.targets(node6, key) shouldEqual targets
          TargetAccessor.targets(node7, key) shouldEqual targets
          TargetAccessor.targets(node8, key) shouldEqual targets
          TargetAccessor.targets(node9, key) shouldEqual targets
        }
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
      success
    }
  }
}