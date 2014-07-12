package server

import akka.actor.{Actor, ActorRef, Props}
import com.google.common.hash.{HashCode, Hashing}
import config.Env

class NodeServiceWorker(node: KeyValNode) extends Actor {
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => sender() ! node.getOperation(o)
    case o @ SetOperation(key, value, t, id) => sender() ! node.setOperation(o)
    case o @ DeleteOperation(key, t, id) => sender() ! node.deleteOperation(o)
    case _ =>
  }
}

class NodeService(node: KeyValNode) extends Actor {
  var workers = List[ActorRef]()
  override def preStart(): Unit = {
    for(i <- 0 to Env.workers) {
      workers = workers :+ context.system.actorOf(Props(classOf[NodeServiceWorker], node))
    }
  }
  def worker(key: String) = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.workers)
    workers(id % Env.workers)
  }
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => worker(key) forward o
    case o @ SetOperation(key, value, t, id) => worker(key) forward o
    case o @ DeleteOperation(key, t, id) => worker(key) forward o
    case r @ Rollback(status) => {
      val ctx = node.env.rollback
      node.locks.putIfAbsent(status.key, ())
      // Rollback management : here be dragons
      // Todo : use timestamp to check if rollback
      val opt = node.getOperation(GetOperation(status.key, 0L, 0L)).value
      if (opt.isDefined && status.old.isEmpty) node.deleteOperation(DeleteOperation(status.key, 0L, 0L))
      else if (opt.isDefined && status.old.isDefined && opt.get == status.value.get) node.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
      else if (opt.isEmpty && status.old.isDefined) node.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
      node.locks.remove(status.key)
      ctx.close()
    }
    case SyncCacheAndBalance() => {
      node.syncCacheIfNecessary(true)
      node.rebalance()
    }
    case _ =>
  }
}