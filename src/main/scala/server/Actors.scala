package server

import akka.actor.{Actor, ActorRef, Props}
import com.google.common.hash.{HashCode, Hashing}
import config.Env

class NodeServiceWorker(node: KeyValNode) extends Actor {
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => sender() ! node.db.getOperation(o)
    case o @ SetOperation(key, value, t, id) => sender() ! node.db.setOperation(o)
    case o @ DeleteOperation(key, t, id) => sender() ! node.db.deleteOperation(o)
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
      node.lock(status.key)
      // Rollback management : here be dragons
      // Todo : use timestamp to check if rollback
      val opt = node.db.getOperation(GetOperation(status.key, 0L, 0L)).value
      if (opt.isDefined && status.old.isEmpty) node.db.deleteOperation(DeleteOperation(status.key, 0L, 0L))
      else if (opt.isDefined && status.old.isDefined && opt.get == status.value.get) node.db.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
      else if (opt.isEmpty && status.old.isDefined) node.db.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
      node.unlock(status.key)
      ctx.close()
    }
    case SyncCacheAndBalance() => {
      node.db.forceSync()
      node.rebalance()
    }
    case _ =>
  }
}