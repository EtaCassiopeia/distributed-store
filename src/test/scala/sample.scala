import java.util.concurrent.Executors

import config.ClusterEnv
import play.api.libs.json.Json
import server.{KeyValNode, NodeClient}

import scala.concurrent.ExecutionContext

object ReadmeSample extends App {

  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  val nbrOfReplicates = 3

  val env = ClusterEnv(nbrOfReplicates)
  val node1 = KeyValNode("node1", env).start("127.0.0.1", 7000)  // nodes can be started on different physical nodes
  val node2 = KeyValNode("node2", env).start(seedNodes = Seq("127.0.0.1:7000"))
  val node3 = KeyValNode("node3", env).start(seedNodes = Seq("127.0.0.1:7000"))
  val node4 = KeyValNode("node4", env).start(seedNodes = Seq("127.0.0.1:7000"))
  val node5 = KeyValNode("node5", env).start(seedNodes = Seq("127.0.0.1:7000"))

  val client = NodeClient(env).start(seedNodes = Seq("127.0.0.1:7000"))

  val result = for {
    _ <- client.set("key1", Json.obj("hello" -> "world"))  // persisted on 3 nodes
    _ <- client.set("key99", Json.obj("goodbye" -> "world")) // persisted on 3 nodes
    _ <- client.set("key50", Json.obj("mehhh" -> "world")) // persisted on 3 nodes
    key1 <- client.get("key1")
    key99 <- client.get("key99")
    key50 <- client.get("key50")
    _ <- client.delete("key1")
    _ <- client.delete("key50")
    _ <- client.delete("key99")
  } yield (key1, key50, key99)

  result.map {
    case (key1, key50, key99) =>
      println(key1.map(Json.stringify).getOrElse("Fail to read key1"))
      println(key50.map(Json.stringify).getOrElse("Fail to read key50"))
      println(key99.map(Json.stringify).getOrElse("Fail to read key99"))
  } andThen {
    case _ =>
      client.stop()
      node1.stop()
      node2.stop()
      node3.stop()
      node4.stop()
      node5.stop()
  }
}
