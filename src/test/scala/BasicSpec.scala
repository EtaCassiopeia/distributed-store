import java.util.concurrent.{TimeUnit, Executors}

import common.IdGenerator
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json.Json
import server.{NodeClient, ClusterEnv, KeyValNode}

import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.Duration

class BasicSpec extends Specification with Tags {
  sequential

  "Distributed Map" should {

    implicit val timeout = Duration(10, TimeUnit.SECONDS)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    val env = ClusterEnv(0)
    val node1 = KeyValNode(s"node1-${IdGenerator.token(6)}", env)
    val client = NodeClient(env)

    "Start a node" in {
      node1.start()
      client.start()
      Thread.sleep(2000)   // Wait for cluster setup
      success
    }

    "Insert some stuff" in {

      def insert(key: String) = Await.result( client.set(key, Json.obj(
        "Hello" -> "World", "key" -> key
      )), timeout)

      insert("12341")
      insert("12342")
      insert("12343")
      insert("12344")
      insert("12345")
      insert("12346")
      insert("12347")
      insert("12348")
      insert("12349")
      insert("12340")

      success
    }

    "Read some stuff" in {
      def shouldFetch(key: String) = {
        val expected = Some(Json.obj("Hello" -> "World", "key" -> key))
        val res = Await.result(client.get(key), timeout)
        println(Json.prettyPrint(res.get))
        res shouldEqual expected
      }
      shouldFetch("12341")
      shouldFetch("12342")
      shouldFetch("12343")
      shouldFetch("12344")
      shouldFetch("12345")
      shouldFetch("12346")
      shouldFetch("12347")
      shouldFetch("12348")
      shouldFetch("12349")
      shouldFetch("12340")
      success
    }

    "Delete stuff" in {
      Await.result(client.delete("12341"), timeout)
      Await.result(client.delete("12342"), timeout)
      Await.result(client.delete("12343"), timeout)
      Await.result(client.delete("12344"), timeout)
      Await.result(client.delete("12345"), timeout)
      Await.result(client.delete("12346"), timeout)
      Await.result(client.delete("12347"), timeout)
      Await.result(client.delete("12348"), timeout)
      Await.result(client.delete("12349"), timeout)
      Await.result(client.delete("12340"), timeout)
      success
    }

    "Stop the node" in {
      node1.displayStats().stop().destroy()
      client.stop()
      Thread.sleep(2000)
      success
    }
  }
}

class ConcurrentUsageSpec extends Specification with Tags {
  sequential

  "Distributed Map" should {

    val nbrClients = 8
    val nbrNodes = 4
    val nbrReplicates = 2
    implicit val timeout = Duration(10, TimeUnit.SECONDS)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    val userEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nbrClients))

    val env = ClusterEnv(nbrReplicates)
    var nodes = List[KeyValNode]()
    (0 to nbrNodes).foreach { i =>
      nodes = nodes :+ KeyValNode(s"node$i-${IdGenerator.token(6)}", env)
    }
    val client = NodeClient(env)

    "Start nodes" in {
      nodes.foreach(_.start())
      client.start()
      Thread.sleep(10000)   // Wait for cluster setup
      env.start()
      success
    }

    "Let user do some stuff" in {

      def scenario: Unit = {
        for (i <- 0 to 100) {
          var seq = Seq[String]()
          for (j <- 0 to 100) {
            val id = IdGenerator.uuid
            seq = seq :+ id
            Await.result(client.set(id)(Json.obj("hello" -> "world", "id" -> id, "stuff1" -> IdGenerator.extendedToken(256), "stuff2" -> IdGenerator.extendedToken(256))), timeout)
          }
          seq.foreach { id =>
            Await.result(client.get(id), timeout) should not beNone
          }
          seq.foreach { id =>
            Await.result(client.delete(id), timeout)
          }
        }
      }
      var list = List[Future[Unit]]()
      for (i <- 0 to nbrClients) {
        list = list :+ Future(scenario)(userEc)
      }
      val fu = Future.sequence(list)
      Await.result(fu, Duration(600, TimeUnit.SECONDS))
      success
    }

    "Stop the nodes" in {
      nodes.foreach(_.displayStats().stop().destroy())
      client.stop()
      Thread.sleep(2000)
      env.stop()
      success
    }
  }
}