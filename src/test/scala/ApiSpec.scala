import java.util.concurrent.{Executors, TimeUnit}

import common.IdGenerator
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server.{ClusterEnv, KeyValNode, NodeClient}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

package object server {
  object TargetAccessor {
    def targets(node: KeyValNode, key: String) = {
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

    val env = ClusterEnv(4)
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
    var keys = Seq[String]()

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
      Thread.sleep(10000)   // Wait for cluster setup
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