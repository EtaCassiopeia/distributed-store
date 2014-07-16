package server

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import akka.actor.{Actor, ActorRef, Props}
import com.google.common.hash.{HashCode, Hashing}
import config.Env

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try


class NodeServiceWorker(id: String, node: KeyValNode) extends Actor {

  def performRollback() = {
    if (RollbackService.pendingRollback.containsKey(id)) {
      val roll = RollbackService.pendingRollback.get(id)
      RollbackService.pendingRollback.remove(id)
      roll.block()
      roll.trigger.success(())
    }
  }

  override def receive: Receive = {
    case o @ GetOperation(key, t, id, start) => {
      performRollback()
      sender() ! node.db.getOperation(o)
      node.metrics.endAwait(start)
    }
    case o @  SetOperation(key, value, t, id, start) => {
      performRollback()
      sender() ! node.db.setOperation(o)
      node.metrics.endAwait(start)
    }
    case o @ DeleteOperation(key, t, id, start) => {
      performRollback()
      sender() ! node.db.deleteOperation(o)
      node.metrics.endAwait(start)
    }
    case o @ RollbackPusher(_) => performRollback()
    case _ =>
  }
}

class NodeService(node: KeyValNode) extends Actor {
  var workers = List[ActorRef]()
  override def preStart(): Unit = {
    for(i <- 0 to Env.workers) {
      workers = workers :+ context.system.actorOf(Props(classOf[NodeServiceWorker], s"${i}", node))
    }
  }
  def worker(key: String) = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.workers)
    workers(id % Env.workers)
  }
  override def receive: Receive = {
    case o @ GetOperation(key, _, _, _) => worker(key) forward o
    case o @ SetOperation(key, _, _, _, _) => worker(key) forward o
    case o @ DeleteOperation(key, _, _, _) => worker(key) forward o
    case o @ RollbackPusher(key) => worker(key) forward o
    case SyncCacheAndBalance() => {
      node.db.forceSync()
      node.rebalance()
    }
    case _ =>
  }
}

object RollbackService {
  val pendingRollback = new ConcurrentHashMap[String, RollbackOperation]()
}

class RollbackService(node: KeyValNode) extends Actor {
  override def receive: Actor.Receive = {
    case r @ Rollback(status) => {
      val ctx = node.metrics.rollback
      def performRollback() = {
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
      val promise = Promise[Unit]()
      RollbackService.pendingRollback.put(status.key, RollbackOperation(promise, performRollback))
      node.system().actorSelection(s"/user/${Env.mapService}") ! RollbackPusher(status.key)
      Try { Await.result(promise.future, Duration(1, TimeUnit.MINUTES)) }
    }
    case _ =>
  }
}