package common

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.util.Timeout
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import akka.cluster.{Cluster, Member}
import java.util.concurrent.atomic.AtomicLong
import config.Env

import scala.util.Try
import scala.concurrent.Future
import scala.reflect.ClassTag
import collection.JavaConversions._

class ClusterStateClient extends Actor {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case MemberUp(member) => {
      Logger("ClusterStateClient").debug("Member Up")
      Client.addMember(member)
      Client.displayState()
    }
    case UnreachableMember(member) => {
      Logger("ClusterStateClient").debug("Member Down")
      Client.removeMember(member)
      Client.displayState()
    }
    case MemberRemoved(member, previousStatus) => {
      Logger("ClusterStateClient").debug("Member Down")
      Client.removeMember(member)
      Client.displayState()
    }
    case _: MemberEvent =>
  }
}

object Client {
  private[this] val membersList = new ConcurrentHashMap[String, Member]()

  def addMember(member: Member) = {
    if (!membersList.containsKey(member)) membersList.put(member.address.toString, member)
  }
  def removeMember(member: Member) = if (membersList.containsKey(member.address.toString)) membersList.remove(member.address.toString)
  def members(): List[Member] = membersList.values().toList.sortBy(_.address.toString)

  def displayState() = {
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
    Logger("CLIENTS_WATCHER").debug(s"Cluster members are : ")
    members().foreach { member =>
      Logger("CLIENTS_WATCHER").debug(s"==> ${member.address} :: ${member.getRoles} => ${member.status}")
    }
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
  }

  def init() = system().actorOf(Props[ClusterStateClient])

  val system = Reference.empty[ActorSystem]()
  val cluster = Reference.empty[Cluster]()

  def ref(name: String): Client = new Client(name, system(), Env.longTimeout, Some(cluster()), Set[String]())
  def apply() = new ClientBuilder(system(), Env.longTimeout, Some(cluster()), Set[String]())
}

class ClientBuilder(system: ActorSystem, timeout: Timeout, cluster: Option[Cluster], roles: Set[String]) {
  def withActorSystem(system: ActorSystem) = new ClientBuilder(system, timeout, cluster, roles)
  def withCluster(cluster: Cluster) = new ClientBuilder(system, timeout, Some(cluster), roles)
  def withNoCluster() = new ClientBuilder(system, timeout, None, roles)
  def withTimeout(timeout: Timeout) = new ClientBuilder(system, timeout, cluster, roles)
  def withRole(role: String) = new ClientBuilder(system, timeout, cluster, Set(role))
  def withRoles(roles: Set[String]) = new ClientBuilder(system, timeout, cluster, roles)
  def ref(name: String): Client = new Client(name, system, timeout, cluster, roles)
}

class Client(name: String, system: ActorSystem, timeout: Timeout, cluster: Option[Cluster] = None, roles: Set[String] = Set[String]()) {
  import collection.JavaConversions._

  private[this] val next = new AtomicLong(0)

  private[this] def next(membersListSize: Int): Int = {
    (next.getAndIncrement % (if (membersListSize > 0) membersListSize else 1)).toInt
  }

  private[this] def next(members: List[Member]): Option[Member] = Try(members(next(members.size))).toOption

  private[this] def roleMatches(member: Member): Boolean = {
    //println(s"[CLIENT_DEBUG] search : $roles, member : ${member.getRoles.toSet}")
    if (roles.isEmpty) true
    else roles.intersect(member.getRoles.toSet).nonEmpty
  }
  private[this] def meIncluded(member: Member): Boolean = true
  private[this] def meNotIncluded(member: Member): Boolean = member.address != cluster.get.selfAddress

  def !(message: Any, sender: ActorRef = system.deadLetters): Future[Unit] = tell(message, sender)

  def tell(message: Any, sender: ActorRef = system.deadLetters): Future[Unit] = {
    Logger("AKKA-LB-CLIENTS").debug(s"Calling service '/user/$name' with '$message' ")
    cluster.map { c =>
      next(Client.members().filter(roleMatches)).foreach { member =>
        Logger("AKKA-LB-CLIENTS").debug(s"Calling service '/user/$name' with '$message' on '${member.address}'")
        system.actorSelection(RootActorPath(member.address) / "user" / name).tell(message, sender)
      }
    } getOrElse system.actorSelection("/user/" + name).tell(message, sender)
    Future.successful(())
  }

  def ask[T](message: Any)(implicit timeout: Timeout = timeout, tag: ClassTag[T]): Future[Option[T]] = {
    cluster.map {  c =>
      next(Client.members().filter(roleMatches)).map { member =>
        Logger("AKKA-LB-CLIENTS").debug(s"Calling service '/user/$name' with '$message' on '${member.address}'")
        akka.pattern.ask(system.actorSelection(RootActorPath(member.address) / "user" / name), message)(timeout).mapTo[T](tag).map(Some(_))(system.dispatcher)
      } getOrElse Future.successful(None)
    } getOrElse akka.pattern.ask(system.actorSelection("/user/" + name), message)(timeout).mapTo[T](tag).map(Some(_))(system.dispatcher)
  }

  def !!(message: Any, sender: ActorRef = system.deadLetters): Future[Unit] = broadcast(message, sender)

  def broadcast(message: Any, sender: ActorRef = system.deadLetters): Future[Unit] = {
    if (cluster.isEmpty) tell(message, sender)
    else cluster.map { c =>
      Client.members().filter(roleMatches).filter(meNotIncluded).foreach { member =>
        Logger("AKKA-LB-CLIENTS").debug(s"Calling service '/user/$name' with '$message' on '${member.address}'")
        system.actorSelection(RootActorPath(member.address) / name).tell(message, sender)
      }
    }
    Future.successful(())
  }

  def !!!(message: Any, sender: ActorRef = system.deadLetters): Future[Unit] = broadcastAll(message, sender)

  def broadcastAll(message: Any, sender: ActorRef = system.deadLetters): Future[Unit] = {
    if (cluster.isEmpty) tell(message, sender)
    else cluster.map { c =>
      Client.members().filter(roleMatches).filter(meIncluded).foreach { member =>
        Logger("AKKA-LB-CLIENTS").debug(s"Calling service '/user/$name' with '$message' on '${member.address}'")
        system.actorSelection(RootActorPath(member.address) / name).tell(message, sender)
      }
    }
    Future.successful(())
  }
}