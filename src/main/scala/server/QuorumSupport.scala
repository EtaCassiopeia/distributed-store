package server

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSelection, ActorSystem, Address, RootActorPath}
import akka.cluster.Member
import akka.pattern.ask
import com.codahale.metrics.Timer
import common.flatfutures._
import common.{Futures, Logger}
import config.Env
import play.api.libs.json.Json
import server.messages.ResponseProto.Response

import scala.collection.JavaConversions._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

trait QuorumSupport { self: KeyValNode =>

  case class LocalizedStatus(ops: OpStatus, member: Member)

  private[server] def performOperationWithQuorum(op: Operation, targets: Seq[Member]): Future[OpStatus] = {
    val quorumNbr = op match {
      case _: GetOperation => env.quorumRead
      case _ => env.quorumWrite
    }
    performOperationWithQuorumAndTrigger(op, targets, quorumNbr)
  }

  private[this] def extractQuorum(op: Operation, lll: Seq[LocalizedStatus], quorumNbr: Int, actorSys: ActorSystem, address: Address, selection: ActorSelection): OpStatus = {
    val ctx2 = metrics.startQuorum
    val fuStatuses = lll.map(_.ops)
    val successfulStatuses = fuStatuses.toList.filter(_.successful).sortWith {(r1, r2) => r1.value.isDefined }
    if (successfulStatuses.size < quorumNbr) {
      Logger.trace(s"Operation failed : quorum was ${successfulStatuses.size} success / $quorumNbr mandatory")
      ctx2.close()
      OpStatus(false, op.key, None, op.timestamp, op.operationId)
    } else {
      val ops: OpStatus = successfulStatuses.headOption match {
        case Some(first) => {
          // check if all responses equals first one
          val valid = successfulStatuses.filter(_.value == first.value).map(_ => 1).fold(0)(_ + _) // TODO : handle version timestamp conflicts
          // if better than quorum then return first OpStatus
          if (valid >= quorumNbr) first
          else {
            Logger.trace(s"Operation failed : quorum was $valid success / $quorumNbr mandatory")
            Logger.trace(fuStatuses.toString())
            // Transaction rollback here, tell every nodes to rollback to it's previous state
            val fail = OpStatus(false, first.key, None, first.timestamp, first.operationId)
            def performRollback(status: LocalizedStatus) = {
              if (status.member.address == address) {
                selection ! Rollback(status.ops)
              } else {
                actorSys.actorSelection(RootActorPath(status.member.address) / "user" / NodeCell.cellName(status.ops.key))
              }
            }
            op match {
              //case DeleteOperation(_, _, _, _) => lll.foreach( status => system().actorSelection(RootActorPath(status.member.address) / "user" / Env.rollbackService) ! Rollback(status.ops))
              case DeleteOperation(_, _, _, _) => lll.foreach(performRollback)
              case SetOperation(_, _, _, _, _) => lll.foreach(performRollback)
              case _ =>
            }
            fail
          }
        }
        case None => {
          Logger.error(s"Operation failed : no response !!!")
          OpStatus(false, "None", None, System.currentTimeMillis(), 0L)
        }
      }
      ctx2.close()
      ops
    }
  }

  private[this] def responseToStatus(res: Response) = {
    OpStatus(res.getSuc, res.getKey, if (res.hasValue) Some(res.getValue.toByteArray) else None, res.getTime, res.getId, if (res.hasOld) Some(res.getOld.toByteArray) else None)
  }

  private[this] def performOperationWithQuorumAndTrigger(op: Operation, targets: Seq[Member], quorumNbr: Int): Future[OpStatus] = {
    val actorSys = system()
    val selection = NodeCell.cellName(op.key, actorSys)
    implicit val ec = system().dispatcher
    val address = cluster().selfAddress
    val trigger = new AtomicInteger(quorumNbr)
    val promise = Promise[OpStatus]()
    val queue = new ConcurrentLinkedQueue[LocalizedStatus]()
    def tryCompleteOperation(st: LocalizedStatus, ctx: Timer.Context) = {
      queue.offer(st)
      if (trigger.decrementAndGet() == 0) {
        promise.trySuccess(extractQuorum(op, queue.toSeq, quorumNbr, actorSys, address, selection))
        ctx.close()
      }
    }
    val ctx1 = metrics.startQuorumAggr
    targets.map { member =>
      val ctx3 = metrics.startRemoting
      if (member.address == address) {
        selection.ask(op.toProtobuf)(Env.longTimeout).mapTo[Response].map(responseToStatus).map(LocalizedStatus(_, member)).recover {
          case _ => LocalizedStatus(OpStatus(false, "", None, System.currentTimeMillis(), 0L), member)
        }.andThen {
          case Success(e) => {
            ctx3.close()
            tryCompleteOperation(e, ctx1)
          }
          case Failure(e) => {
            ctx3.close()
            promise.tryFailure(e)
          }
        }
      } else {
        actorSys.actorSelection(RootActorPath(member.address) / "user" / NodeCell.cellName(op.key)).ask(op.toProtobuf)(Env.longTimeout).mapTo[Response].map(responseToStatus).map(LocalizedStatus(_, member)).recover {
          case _ => LocalizedStatus(OpStatus(false, "", None, System.currentTimeMillis(), 0L), member)
        }.andThen {
          case Success(e) => {
            ctx3.close()
            tryCompleteOperation(e, ctx1)
          }
          case Failure(e) => {
            ctx3.close()
            promise.tryFailure(e)
          }
        }
      }
    }.asFuture.map { lll =>
      if (!promise.isCompleted) {
        ctx1.close()
        val opst = extractQuorum(op, queue.toSeq, quorumNbr, actorSys, address, selection)
        if (!opst.successful) metrics.quorumFailure
        promise.trySuccess(opst)
      }
    }.onFailure {
      case e => promise.tryFailure(e)
    }
    promise.future.andThen {
      case Success(OpStatus(false, _, _, _, _, _)) => metrics.quorumFailure
      case Success(OpStatus(true, _, _, _, _, _)) => metrics.quorumSuccess
      case Failure(_) => metrics.quorumFailure
    }
  }

  private[this] def performOperationWithQuorumNaive(op: Operation, targets: Seq[Member]): Future[OpStatus] = {
    implicit val ec = system().dispatcher
    val quorumNbr = op match {
      case _: GetOperation => env.quorumRead
      case _ => env.quorumWrite
    }
    val actorSys = system()
    val address = cluster().selfAddress
    val selection = NodeCell.cellName(op.key, actorSys)
    def actualOperation(): Future[OpStatus] = {
      val ctx1 = metrics.startQuorumAggr
      targets.map { member =>
        if (member.address == address) {
          selection.ask(op)(Env.longTimeout).mapTo[OpStatus].map(LocalizedStatus(_, member)).recover {
            case _ => LocalizedStatus(OpStatus(false, "", None, System.currentTimeMillis(), 0L), member)
          }
        } else {
          actorSys.actorSelection(RootActorPath(member.address) / "user" / NodeCell.cellName(op.key)).ask(op)(Env.longTimeout).mapTo[OpStatus].map(LocalizedStatus(_, member)).recover {
            case _ => LocalizedStatus(OpStatus(false, "", None, System.currentTimeMillis(), 0L), member)
          }
        }
      }.asFuture.andThen {
        case _ => ctx1.close()
      }.map { lll =>
        extractQuorum(op, lll, quorumNbr, actorSys, address, selection)
      }.andThen {
        case Success(OpStatus(false, _, _, _, _, _)) => metrics.quorumFailure
        case Failure(_) => metrics.quorumFailure
        case Success(OpStatus(true, _, _, _, _, _)) =>
      }
    }
    Futures.retryWithPredicate[OpStatus](10, _.successful)(actualOperation()).andThen {
      case Success(OpStatus(false, _, _, _, _, _)) => metrics.quorumRetryFailure
      case Success(OpStatus(true, _, _, _, _, _)) => metrics.quorumSuccess
      case Failure(_) => metrics.quorumRetryFailure
    }
  }
}
