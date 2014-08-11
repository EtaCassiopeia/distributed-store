package server

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import akka.actor._
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import com.google.common.hash.{HashCode, Hashing}
import com.typesafe.config.Config
import config.Env
import metrics.Metrics

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try

class CellPriorityMailbox(settings: ActorSystem.Settings, config: Config) extends UnboundedPriorityMailbox(
  PriorityGenerator {
    case Rollback(_)   => 0  // High priority message
    case PoisonPill    => 2
    case otherwise     => 1
  }
)

object NodeCell {

  def cellIdx(key: String) = {
    Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.cells)
  }

  def cellDb(key: String, node: KeyValNode) = {
    val id = cellIdx(key)
    node.dbs(id)
  }

  def cellPath(key: String): String = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.cells)
    s"/user/node-cell-$id"
  }

  def cellName(key: String): String = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.cells)
    s"node-cell-$id"
  }

  def cellName(key: String, system: ActorSystem): ActorSelection = system.actorSelection(cellPath(key))

  def formattedName(id: Int) = s"node-cell-$id"
  def formattedPath(id: Int) = s"/user/node-cell-$id"
}

class NodeCell(name: String, db: OnDiskStore, metrics: Metrics) extends Actor {

  override def receive: Receive = {
    case o @ GetOperation(key, t, id, start) => {
      sender() ! db.getOperation(o)
      metrics.endAwait(start)
    }
    case o @  SetOperation(key, value, t, id, start) => {
      sender() ! db.setOperation(o)
      metrics.endAwait(start)
    }
    case o @ DeleteOperation(key, t, id, start) => {
      sender() ! db.deleteOperation(o)
      metrics.endAwait(start)
    }
    // Rollback messages have the highest priority in cells, so rollback happen right away
    case Rollback(status) => {
        val ctx = metrics.rollback
        // Rollback management : here be dragons
        // Todo : use timestamp to check if rollback
        val opt = db.getOperation(GetOperation(status.key, 0L, 0L)).value
        if (opt.isDefined && status.old.isEmpty) db.deleteOperation(DeleteOperation(status.key, 0L, 0L))
        else if (opt.isDefined && status.old.isDefined && opt.get == status.value.get) db.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
        else if (opt.isEmpty && status.old.isDefined) db.setOperation(SetOperation(status.key, status.old.get, 0L, 0L))
        ctx.close()
    }
    case DbForceSync() => db.forceSync()
    case DbClose() => db.close()
    case DbDestroy() => db.destroy()
    case _ =>
  }
}
