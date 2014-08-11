package server

import akka.actor.{Actor, Address}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.google.common.hash.{HashCode, Hashing}
import common.Logger
import config.Env

import scala.collection.JavaConversions._

class NodeClusterWatcher(node: KeyValNode) extends Actor {
  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def receive: Receive = {
    case MemberUp(member) => {
      Logger.info(s"New member ${member.getRoles} is up at ${member.address.toString}")
      node.rebalance()
    }
    case UnreachableMember(member) => {
      node.rebalance()
    }
    case MemberRemoved(member, previousStatus) => {
      node.rebalance()
    }
    case _ =>
  }
}

trait ClusterSupport { self: KeyValNode =>

  private[server] def members(): Seq[Member] = {
    cluster().state.getMembers.toSeq.filter(_.getRoles.contains(Env.nodeRole)).sortWith { (m1, m2) =>
      val a1 = m1.address.toString.hashCode
      val a2 = m2.address.toString.hashCode
      a1.compareTo(a2) < 0
    }
  }

  private[server] def listOfNodes(): Seq[Member] = members()

  private[server] def numberOfNodes(): Int = listOfNodes().size

  // For testing purpose
  private[server] def targets(key: String, replicates: Int): Seq[Address] = targetAndNext(key, replicates).map(_.address)

  private[server] def target(key: String): Member = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    listOfNodes()(id % (if (numberOfNodes() > 0) numberOfNodes() else 1))
  }

  private[server] def targetAndNext(key: String, replicates: Int): Seq[Member] = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    var targets = Seq[Member]()
    for (i <- 0 to replicates - 1) {
      val next = (id + i) % (if (numberOfNodes() > 0) numberOfNodes() else 1)
      targets = targets :+ listOfNodes()(next)
    }
    targets
  }

  private[server] def displayState() = {
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
    Logger("CLIENTS_WATCHER").debug(s"Cluster members are : ")
    members().foreach { member =>
      Logger("CLIENTS_WATCHER").debug(s"==> ${member.address} :: ${member.getRoles} => ${member.status}")
    }
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
  }
}
