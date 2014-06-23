import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import common.{IdGenerator, Logger}
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server.{ClusterEnv, DistributedMapNode, NodeClient}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class NodeDownAndUpSpec extends Specification with Tags {
  sequential

  "Distributed Map" should {

    implicit val timeout = Duration(10, TimeUnit.SECONDS)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    val env = ClusterEnv(4)
    val node1 = DistributedMapNode(s"node1-${IdGenerator.uuid}", env)
    val node2 = DistributedMapNode(s"node2-${IdGenerator.uuid}", env)
    val node3 = DistributedMapNode(s"node3-${IdGenerator.uuid}", env)
    val node4 = DistributedMapNode(s"node4-${IdGenerator.uuid}", env)
    val node5 = DistributedMapNode(s"node5-${IdGenerator.uuid}", env)
    val node6 = DistributedMapNode(s"node6-${IdGenerator.uuid}", env)
    val node7 = DistributedMapNode(s"node7-${IdGenerator.uuid}", env)
    val node8 = DistributedMapNode(s"node8-${IdGenerator.uuid}", env)
    val node9 = DistributedMapNode(s"node9-${IdGenerator.uuid}", env)
    val client = NodeClient(env)
    var keys = Seq[String]()
    val counterOk = new AtomicLong(0L)
    val counterKo = new AtomicLong(0L)

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
      Thread.sleep(6000)   // Wait for cluster setup
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

    "Shutdown some nodes" in {
      node2.displayStats().stop().destroy()
      node4.displayStats().stop().destroy()
      node6.displayStats().stop().destroy()
      Thread.sleep(20000)
      success
    }

    "Insert some stuff again" in {
      for (i <- 0 to 1000) {
        val id = IdGenerator.uuid
        keys = keys :+ id
        Await.result( client.set(id, Json.obj(
          "Hello" -> "World", "key" -> id
        )), timeout)
      }
      success
    }

    "Startup down nodes again" in {
      node2.start()
      node4.start()
      node6.start()
      Thread.sleep(30000)
      success
    }

    "Read some stuff" in {
      keys.foreach { key =>
        val expected = Some(Json.obj("Hello" -> "World", "key" -> key))
        if (Await.result(client.get(key), timeout) == expected) counterOk.incrementAndGet()
        else counterKo.incrementAndGet()
      }
      success
    }

    "Delete stuff" in {
      keys.foreach { key =>
        Await.result(client.delete(key), timeout)
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
      Thread.sleep(5000)
      Logger.info(s"Read OK ${counterOk.get()}")
      Logger.info(s"Read KO ${counterKo.get()}")
      counterKo.get() shouldEqual 0L
      success
    }
  }
}