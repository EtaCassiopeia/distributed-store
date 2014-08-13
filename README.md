Distributed KeyVal Store
===============================

An experiment to produce a distributed and decentralized key/value store (for Json documents, or whatever) 
with masterless automatic replication accross nodes. This project aims at providing a pure Scala API
to build stuff around a replicated key/value store

Date are distributed accross the nodes based on their key (Consistent Hasing) 
and for each operation, data are replicated on/retrieved from the `n` following nodes.
Each operation is validated by a majority of nodes.

Libraries
-----

Various libraries are used in the project

* Scala 2.11
* Akka and akka-cluster module (http://doc.akka.io/docs/akka/2.3.4/common/cluster.html)
* LevelDB (Java impl. https://github.com/dain/leveldb)
* Play Json lib (https://www.playframework.com/documentation/2.3.x/ScalaJson)
* Google Guava (https://code.google.com/p/guava-libraries/)
* Google Protocol Buffers (https://code.google.com/p/protobuf/)
* Typesafe config lib (https://github.com/typesafehub/config)
* Yammer Metrics (http://metrics.codahale.com/)

Cluster
-----

The cluster is provided by the awesome akka-cluster library. 
It provides a fault-tolerant decentralized peer-to-peer based cluster membership service with 
no single point of failure or single point of bottleneck. It does this using gossip protocols and an automatic failure detector. 
Two types of nodes are possible in the cluster, datastore nodes or client nodes. 
Clients are full part of the cluster and are perfectly aware of the cluster topology.

Messaging
-----

Nodes are communicating with each other using akka and its remoting capabilities. 
Messages are serialized using Google Protobuf for better efficiency.
Experiments will be made to use more serious stuff like Apache Thrift and Nifty.
Different messages are used for the various types of operations. A response message is used too.

Data partitions and balancing
-----

Each node own a finite number of data cell (10 by default). Those cells are forming a ring with other cells from cluster nodes and
each cell is responsible for a partition of keys. The location of a key in the cluster is managed using a consistent 
hash algorithm on the cells ring.
Each cell owns its own storage space on disk and it's own commit log file.

If new nodes a added to the cluster while running, the ring will reorganize itself and the nodes will rebalance data between each other.

Operations validation
-----

When data is read or written to the cluster, data is replicated over several nodes for high-availability and durability concerns.
You can configure a number `N` of replicates according to the number of nodes at your disposal.
Each operation is valid only if a majority of replicates agrees on it (a quorum). 

Quorum number can be determined like : `(replicates / 2) + 1`

If the quorum is not reached, then a rollback operation is sent to each node involved in the operation and processed using a priority mailbox.
Transactions does not use locks to work so the behavior can be weird sometimes (you can read something not consistent, but quorum on read should help to avoid that).

You can use two modes of consistency. A sync mode where you actually wait until all response needed for quorum happens. And an async one
where you only wait for the first response to return (but the full quorum process is still happening). Another mode will be added 
where write operations will be fully async, aka no wait at all.  

Data Storage
-----

For a write, the client will first search for the target node and its replicates. An operation message will be send to each of those nodes.
Arrived on a node, the operation is first written in an append-only log to ensure durability. Then, using the key, 
the node will chose in which cell 
the data should be written, and then the data is written in an in-memory structure. Here, the data can be manipulated like any other data. 
When the structure is full (too much data, too much keys), it is persisted on disk (using LevelDB)
and the operation log is rolled. In case of a node crash, the node will first replay all the operations in the log to fill 
the in-memory structure before starting serving requests. Everything persisted on disk will still be there (except if the hard drive is dead, but that's another story)

For a read operation, after node and cell targeting, the data will be searched in the in-memory structure of the cell first. If it's not available there, then the node will search 
in the cell levelDB (on disk, so it's slower).

Metrics
-----

Some useful metrics are expose on the JMX interface of the process

API usage example
----

```scala
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
```
