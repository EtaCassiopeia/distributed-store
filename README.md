Distributed KeyVal Store
===============================

An experiment to produce a distributed and decentralized key value store (for Json documents) 
with masterless automatic replication accross nodes.

Date are distributed accross the nodes based on their key (Consistent Hasing) 
and for each operation, data are replicated on/retrieved from the `n` following nodes.
Each operation is validated by a majority of nodes.

Persistence is managed by a levelDB for each node. Cluster is managed by akka-cluster.

```scala

/**
 * Here every operation on the client is async, so the following example isn't 100% accurate
 */

implicit val timeout = Duration(1, TimeUnit.SECONDS)
implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
val nbrOfReplicates = 2

val env = ClusterEnv(replicates = 2)
val node1 = DistributedMapNode("node1", env).start() 
val node2 = DistributedMapNode("node2", env).start() 
val node3 = DistributedMapNode("node3", env).start() 
val node4 = DistributedMapNode("node4", env).start() 
val node5 = DistributedMapNode("node5", env).start() 

val client = NodeClient(env)

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