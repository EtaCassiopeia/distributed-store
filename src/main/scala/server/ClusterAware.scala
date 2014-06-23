package server

import java.util.concurrent.ConcurrentHashMap

import akka.actor.Address
import akka.cluster.Member
import com.google.common.hash.{HashCode, Hashing}
import common.Logger
import config.Env
import collection.JavaConversions._

trait ClusterAware { self: DistributedMapNode =>

  //private[this] val membersList = new ConcurrentHashMap[String, Member]()

  private[server] def listOfNodes(): Seq[Member] = members().filter(_.getRoles.contains(Env.nodeRole))

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

  //private[server] def addMember(member: Member) = if (!membersList.containsKey(member)) membersList.put(member.address.toString, member)
  //private[server] def removeMember(member: Member) = if (membersList.containsKey(member.address.toString)) membersList.remove(member.address.toString)
  //private[server] def members(): List[Member] = membersList.values().toList.sortBy(_.address.toString)
  private[server] def displayState() = {
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
    Logger("CLIENTS_WATCHER").debug(s"Cluster members are : ")
    members().foreach { member =>
      Logger("CLIENTS_WATCHER").debug(s"==> ${member.address} :: ${member.getRoles} => ${member.status}")
    }
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
  }
}
