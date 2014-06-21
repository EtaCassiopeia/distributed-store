import java.util.concurrent.{Executors, TimeUnit}

import common.{IdGenerator, ExecutionContextExecutorServiceBridge}
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server.DistributedMapNode

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration.Duration

class ApiSpec extends Specification with Tags {
  sequential

  "Distributed Map" should {

    implicit val timeout = Duration(1, TimeUnit.SECONDS)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    val node1 = DistributedMapNode("node1")
    val node2 = DistributedMapNode("node2")
    val node3 = DistributedMapNode("node3")
    val node4 = DistributedMapNode("node4")
    val node5 = DistributedMapNode("node5")
    var keys = Seq[String]()

    "Start the node" in {
      node1.start()
      node2.start()
      node3.start()
      node4.start()
      node5.start()
      Thread.sleep(3000)   // Wait for cluster setup
      success
    }

    "Insert some stuff" in {
      for (i <- 0 to 1000) {
        val id = IdGenerator.uuid
        keys = keys :+ id
        Await.result( node1.set(id, Json.obj(
          "Hello" -> "World", "key" -> id
        )), timeout)
      }
      success
    }

    "Read some stuff" in {
      Await.result(node1.get("key3"), timeout) should beNone
      keys.foreach { key =>
        Await.result(node1.get(key), timeout) shouldEqual Some(Json.obj("Hello" -> "World", "key" -> key))
      }
      success
    }

    "Delete stuff" in {
      keys.foreach { key =>
        Await.result(node1.delete(key), timeout)
      }
      success
    }

    "Always target same nodes in the ring" in {
      keys.foreach { key =>
        val targets = node1.targets(key)
        node2.targets(key) shouldEqual targets
        node3.targets(key) shouldEqual targets
        node4.targets(key) shouldEqual targets
        node5.targets(key) shouldEqual targets
      }
      success
    }

    "Always target same nodes for the same key" in {
      for (i <- 0 to 10) {
        val key = IdGenerator.uuid
        for (j <- 0 to 100) {
          val targets = node1.targets(key)
          node2.targets(key) shouldEqual targets
          node3.targets(key) shouldEqual targets
          node4.targets(key) shouldEqual targets
          node5.targets(key) shouldEqual targets
        }
      }
      success
    }

    "Stop the node" in {
      node1.displayStats().stop().destroy()
      node2.displayStats().stop().destroy()
      node3.displayStats().stop().destroy()
      node4.displayStats().stop().destroy()
      node5.displayStats().stop().destroy()
      success
    }
  }
}