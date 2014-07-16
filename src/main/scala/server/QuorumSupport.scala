package server

import akka.actor.RootActorPath
import akka.cluster.Member
import common.{Futures, Logger}
import config.Env

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import akka.pattern.ask
import common.flatfutures._

trait QuorumSupport { self: KeyValNode =>

  case class LocalizedStatus(ops: OpStatus, member: Member)

  private[server] def performOperationWithQuorum(op: Operation, targets: Seq[Member]): Future[OpStatus] = {
    implicit val ec = system().dispatcher
    val quorumNbr = op match {
      case _: GetOperation => env.quorumRead
      case _ => env.quorumWrite
    }
    val actorSys = system()
    val address = cluster().selfAddress
    val selection = actorSys.actorSelection(s"/user/${Env.mapService}")
    def actualOperation(): Future[OpStatus] = {
      val ctx1 = metrics.startQuorumAggr
      targets.map { member =>
        if (member.address == address) {
          selection.ask(op)(Env.longTimeout).mapTo[OpStatus].map(LocalizedStatus(_, member)).recover {
            case _ => LocalizedStatus(OpStatus(false, "", None, System.currentTimeMillis(), 0L), member)
          }
        } else {
          actorSys.actorSelection(RootActorPath(member.address) / "user" / Env.mapService).ask(op)(Env.longTimeout).mapTo[OpStatus].map(LocalizedStatus(_, member)).recover {
            case _ => LocalizedStatus(OpStatus(false, "", None, System.currentTimeMillis(), 0L), member)
          }
        }
      }.asFuture.andThen {
        case _ => ctx1.close()
      }.map { lll =>
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
              // if less valid than returns try to rebalance data
              if (valid != fuStatuses.size) rebalance()
              // if better than quorum then return first OpStatus
              if (valid >= quorumNbr) first
              else {
                Logger.trace(s"Operation failed : quorum was $valid success / $quorumNbr mandatory")
                Logger.trace(fuStatuses.toString())
                // Transaction rollback here, tell every nodes to rollback to it's previous state
                val fail = OpStatus(false, first.key, None, first.timestamp, first.operationId)
                op match {
                  case DeleteOperation(_, _, _, _) => lll.foreach( status => system().actorSelection(RootActorPath(status.member.address) / "user" / Env.rollbackService) ! Rollback(status.ops))
                  case SetOperation(_, _, _, _, _) => lll.foreach( status => system().actorSelection(RootActorPath(status.member.address) / "user" / Env.rollbackService) ! Rollback(status.ops))
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
