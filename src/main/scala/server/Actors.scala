package server

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import akka.actor._
import com.google.common.hash.{HashCode, Hashing}
import config.Env
import metrics.Metrics

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try

object NodeCell {
  def cellIdx(key: String) = {
    Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.cells)
  }

  def cellDb(key: String, node: KeyValNode) = {
    val id = cellIdx(key)
    node.dbs(id)
  }

  def cellName(key: String): String = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.cells)
    s"/user/cell-node-$id"
  }

  def cellName(key: String, system: ActorSystem): ActorSelection = system.actorSelection(cellName(key))
}

class NodeCell(name: String, db: OnDiskStore, metrics: Metrics) extends Actor {

  def performRollback() = {
    if (RollbackService.pendingRollback.containsKey(name)) {
      val roll = RollbackService.pendingRollback.get(name)
      RollbackService.pendingRollback.remove(name)
      roll.block()
      roll.trigger.success(())
    }
  }

  override def receive: Receive = {
    case o @ GetOperation(key, t, id, start) => {
      performRollback()
      sender() ! db.getOperation(o)
      metrics.endAwait(start)
    }
    case o @  SetOperation(key, value, t, id, start) => {
      performRollback()
      sender() ! db.setOperation(o)
      metrics.endAwait(start)
    }
    case o @ DeleteOperation(key, t, id, start) => {
      performRollback()
      sender() ! db.deleteOperation(o)
      metrics.endAwait(start)
    }
    case RollbackPusher(_) => performRollback()
    case DbForceSync() => db.forceSync()
    case DbClose() => db.close()
    case DbDestroy() => db.destroy()
    case _ =>
  }
}

//

object RollbackService {
  val pendingRollback = new ConcurrentHashMap[String, RollbackOperation]()
}

class RollbackService(node: KeyValNode) extends Actor {
  override def receive: Actor.Receive = {
    case r @ Rollback(status) => {
      val ctx = node.metrics.rollback
      def performRollback() = {
        node.lock(status.key)
        val db = NodeCell.cellDb(status.key, node)
        // Rollback management : here be dragons
        // TODO : find a more actor-ish way
        // Todo : use timestamp to check if rollback
        val opt = db.getOperation(GetOperation(status.key, 0L, 0L)).value
        if (opt.isDefined && status.old.isEmpty) db.deleteOperation(DeleteOperation(status.key, 0L, 0L))
        else if (opt.isDefined && status.old.isDefined && opt.get == status.value.get) db.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
        else if (opt.isEmpty && status.old.isDefined) db.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
        node.unlock(status.key)
        ctx.close()
      }
      val promise = Promise[Unit]()
      RollbackService.pendingRollback.put(status.key, RollbackOperation(promise, performRollback))
      NodeCell.cellName(status.key, node.system()) ! RollbackPusher(status.key)
      Try { Await.result(promise.future, Duration(1, TimeUnit.MINUTES)) }
    }
    case _ =>
  }
}