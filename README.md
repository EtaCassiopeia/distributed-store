Distributed KeyVal Store
===============================

An experiment to produce a distributed and decentralized key/value store (for Json documents, or whatever) 
with masterless automatic replication accross nodes. This project aims at providing a pure Scala API
to build stuff around a replicated key/value store

Date are distributed accross the nodes based on their key (Consistent Hasing) 
and for each operation, data are replicated on/retrieved from the `n` following nodes.
Each operation is validated by a majority of nodes.

Techs
-----

Various techs are on this project

* Scala 2.11
* Akka and akka-cluster module
* Level DB
* Play Json lib
* Guava
* Typesafe config lib
* Metrics

Cluster
-----

The cluster is provided by the awesome akka-cluster library (http://doc.akka.io/docs/akka/2.3.4/common/cluster.html). It provides a fault-tolerant decentralized peer-to-peer based cluster membership service with no single point of failure or single point of bottleneck. It does this using gossip protocols and an automatic failure detector. 
Two types of nodes are possible in the cluster, datastore nodes or client nodes. Client are full part of the cluster and are perfectly aware of the cluster topology.

Messaging
-----

Nodes are communicating with each other using akka and its remoting capabilities. 
Messages are serialized using Google Protobuf for better efficiency.
Experiments will be made to use more heavy stuff like Apache Thrift and Nifty.

Data partitions and balancing
-----

Each node own a finite number of data cell (10 by default). Those cells are forming a ring with other cells from cluster nodes and
each cell is responsible for a partition of keys. The location of a key in the cluster is managed using a consistent hash algorithm on the cells ring.
Each cell owns its own storage space on disk and it's own commit log file.

If new nodes a added to the cluster while running, the ring will reoganize itself and the nodes will rebalances data between each other.

Operations validation
-----

When data is fetched or written to the cluster, data is replicated over several nodes for high-availability and durability concerns.
You can configure a number N of replicates according to the number of nodes at your disposal.
Each operation is valid only if a majority of replicates agrees on it (a quorum). 

Quorum number can be determined like : `(replicates / 2) + 1`

If the quorum is not reached, then a rollback operation is launch on each node involved in the operation using a priority mailbox.

You can use two modes of consistency. A sync mode where you actually wait until all response needed for quorum happens. And an async one
where you only wait for the first response to return (but the full quorum process is still happening). Another mode will be added where write operations will be fully async, aka no wait at all.  

Data Storage
-----

When a write operation happens on a node, it's first written in an operation append-only log to ensure durability. Then the data is written
in a memory structure that can be queried. When the structure is full (too much data, too much keys), it is persisted on disk (using LevelDB)
and the operation log is rolled. In case of a node crash, the node will replay first its operation log before starting serving requests.

When a read operation is performed, the data will be searched in the in-memory structure first. If it's not available there, then the node will search 
in the levelDB on disk.

Metrics
-----

Some useful metrics are expose on the JMX interface of the process

API usage example
----

```scala

/**
 * Here every operation on the client is async, so the following example isn't 100% accurate
 */

implicit val timeout = Duration(1, TimeUnit.SECONDS)
implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
val nbrOfReplicates = 2

val env = ClusterEnv(replicates = 2)
val node1 = KeyValNode("node1", env).start()  // nodes can be started on different physical nodes
val node2 = KeyValNode("node2", env).start() 
val node3 = KeyValNode("node3", env).start() 
val node4 = KeyValNode("node4", env).start() 
val node5 = KeyValNode("node5", env).start() 

val client = NodeClient(env).start()

client.set("key1", Json.obj("hello" -> "world"))  // persisted on n + 1 nodes  
client.set("key99", Json.obj("goodbye" -> "world")) // persisted on n + 1 nodes  

node3.stop() // Data are rebalanced across nodes

client.set("key50", Json.obj("mehhh" -> "world")) // persisted on n + 1 nodes  
client.get("key1") match {
    case Some(doc) => println(Json.stringify(doc))
    case None => println("Fail !!!") 
}

node3.start() // Data are rebalanced across nodes

client.get("key99") match {
    case Some(doc) => println(Json.stringify(doc))
    case None => println("Fail !!!") 
}
client.get("key50") match {
    case Some(doc) => println(Json.stringify(doc))
    case None => println("Fail !!!") 
}

client.delete("key1")
client.delete("key50")
client.delete("key99")

client.stop()
node1.stop()
node2.stop()
node3.stop()
node4.stop()
node5.stop()

```
