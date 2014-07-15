package server

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

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
      node.fetchAndUpdate()
      node.rebalance()
    }
    case UnreachableMember(member) => {
      node.fetchAndUpdate()
      node.rebalance()
    }
    case MemberRemoved(member, previousStatus) => {
      node.fetchAndUpdate()
      node.rebalance()
    }
    case _ =>
  }
}

trait ClusterSupport { self: KeyValNode =>

  private[server] val membersCache = new ConcurrentLinkedQueue[Member]()
  private[server] val murmurCache = new ConcurrentHashMap[String, Long]()

  private[server] def members(): Seq[Member] = {
    if (membersCache.size == 0) {
      val m = updateMembers()
      membersCache.clear()
      membersCache.addAll(m)
    }
    membersCache.toSeq
  }

  private[server] def fetchAndUpdate() {
    val m = updateMembers()
    membersCache.clear()
    membersCache.addAll(m)
  }

  private def updateMembers(): Seq[Member] = {
    val murmur = Hashing.murmur3_128()
    cluster().state.getMembers.toSeq.filter(_.getRoles.contains(Env.nodeRole)).sortWith { (member1, member2) =>
      val key1 = member1.address.toString
      val key2 = member2.address.toString
      val hash1 = Option(murmurCache.get(key1)).getOrElse {
        val m = murmur.hashString(key1, Env.UTF8).asLong()
        murmurCache.putIfAbsent(key1, m)
        m
      }
      val hash2 = Option(murmurCache.get(key2)).getOrElse {
        val m = murmur.hashString(key2, Env.UTF8).asLong()
        murmurCache.putIfAbsent(key2, m)
        m
      }
      hash1.compareTo(hash2) < 0
    }
  }

  private[server] def listOfNodes(): Seq[Member] = members()

  private[server] def numberOfNodes(): Int = listOfNodes().size

  // For testing purpose
  private[server] def targets(key: String): Seq[Address] = targetAndNext(key).map(_.address)

  private[server] def target(key: String): Member = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    listOfNodes()(id % (if (numberOfNodes() > 0) numberOfNodes() else 1))
  }

  private[server] def targetAndNext(key: String): Seq[Member] = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    var targets = Seq[Member]()
    for (i <- 0 to self.replicatesNbr()) {
      val next = (id + i) % (if (numberOfNodes() > 0) numberOfNodes() else 1)
      targets = targets :+ listOfNodes()(next)
    }
    targets
  }

  private[server] def quorum(): Int = ((self.replicatesNbr() + 1) / 2) + 1

  private[server] def displayState() = {
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
    Logger("CLIENTS_WATCHER").debug(s"Cluster members are : ")
    members().foreach { member =>
      Logger("CLIENTS_WATCHER").debug(s"==> ${member.address} :: ${member.getRoles} => ${member.status}")
    }
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
  }
}
