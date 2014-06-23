package server

import akka.actor.Address
import akka.cluster.Member
import com.google.common.hash.{HashCode, Hashing}
import common.{Reference, Logger}
import config.Env
import collection.JavaConversions._

trait ClusterAware { self: DistributedMapNode =>

  private[server] val membersCache = Reference.empty[Seq[Member]]()
  private[server] val nodesCache = Reference.empty[Seq[Member]]()

  private[server] def members(): Seq[Member] = membersCache.getOrElse(updateMembers())

  private[server] def updateNodes(): Seq[Member] = {
    nodesCache <== members().filter(_.getRoles.contains(Env.nodeRole))
    counterCacheSync.incrementAndGet()
    nodesCache()
  }

  private[server] def updateMembers(): Seq[Member] = {
    //val md5 = Hashing.md5()
    membersCache <== cluster().state.getMembers.toSeq.sortWith { (member1, member2) =>
      val hash1 = member1.address.toString.hashCode //md5.hashString(member1.address.toString, Env.UTF8).asLong()
      val hash2 = member2.address.toString.hashCode //md5.hashString(member2.address.toString, Env.UTF8).asLong()
      hash1.compareTo(hash2) < 0
    }
    counterCacheSync.incrementAndGet()
    membersCache()
  }

  private[server] def listOfNodes(): Seq[Member] = members().filter(_.getRoles.contains(Env.nodeRole))//nodesCache.getOrElse(updateNodes())

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
