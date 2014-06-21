package server

import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import akka.util.Timeout
import com.google.common.hash.{HashCode, Hashing}
import com.typesafe.config.{Config, ConfigFactory}
import common._
import config.Env
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}
import collection.JavaConversions._

class DistributionService(system: ActorSystem, replicates: Int) {

  val UTF8 = Charset.forName("UTF-8")
  val timeout = Timeout(5, TimeUnit.SECONDS)

  def listOfNodes(): Seq[Member] = Client.members().filter(_.getRoles.contains(Env.nodeRole))

  def numberOfNodes(): Int = listOfNodes().size

  def target(key: String): Member = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    listOfNodes()(id % (if (numberOfNodes() > 0) numberOfNodes() else 1))
  }

  def targetAndNext(key: String): Seq[Member] = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    var targets = Seq[Member]()
    for (i <- 0 to replicates + 1) {
      val next = (id + i) % (if (numberOfNodes() > 0) numberOfNodes() else 1)
      targets = targets :+ listOfNodes()(next)
    }
    targets
  }

  def quorum(): Int = ((replicates + 1) / 2) + 1

  def performAndWaitForQuorum(op: Operation, targets: Seq[Member]): Future[OpStatus] = {
    implicit val ec = system.dispatcher
    Future.sequence(targets.map { member =>
      system.actorSelection(RootActorPath(member.address) / "user" / Env.mapService).ask(op)(timeout).mapTo[OpStatus].recover {
        case _ => OpStatus(false, "", None, System.currentTimeMillis(), 0L)
      }
    }).map { fuStatuses =>
      val successfulStatuses = fuStatuses.filter(_.successful)
      val first = successfulStatuses.head
      val valid = successfulStatuses.filter(_.value == first.value).map(_ => 1).fold(0)(_ + _)
      if (valid >= quorum()) first
      else {
        Logger.debug("Operation failed :")
        Logger.debug(s"Head was $first")
        Logger.debug(s"Valid was $valid/${quorum()}")
        OpStatus(false, first.key, None, first.operationId, first.timestamp)
      }
    }
  }
}

class NodeService(node: DistributedMapNode) extends Actor {
  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => sender() ! node.getOperation(o)
    case o @ SetOperation(key, value, t, id) => sender() ! node.setOperation(o)
    case o @ DeleteOperation(key, t, id) => sender() ! node.deleteOperation(o)
    case MemberUp(member) => {
      node.addMember(member)
      node.displayState()
      node.rebalance()
    }
    case UnreachableMember(member) => {
      node.removeMember(member)
      node.displayState()
      node.rebalance()
    }
    case MemberRemoved(member, previousStatus) => {
      node.removeMember(member)
      node.displayState()
      node.rebalance()
    }
    case _ =>
  }
}

class DistributedMapNode(name: String, replicates: Int = 2, config: Configuration, path: File, clientOnly: Boolean = false) {

  private[this] val db = Reference.empty[DB]()
  private[this] val node = Reference.empty[ActorRef]()
  private[this] val bootSystem = Reference.empty[ActorSystem]()
  private[this] val system = Reference.empty[ActorSystem]()
  private[this] val cluster = Reference.empty[Cluster]()
  private[this] val service = Reference.empty[DistributionService]()
  private[this] val counterRead = new AtomicLong(0L)
  private[this] val counterWrite = new AtomicLong(0L)
  private[this] val counterDelete = new AtomicLong(0L)
  private[this] val generator = IdGenerator(Random.nextInt(1024))
  private[this] val options = new Options()
  private[this] val membersList = new ConcurrentHashMap[String, Member]()

  options.createIfMissing(true)
  Logger.configure()

  if (replicates < 2) throw new RuntimeException("Cannot have less than 2 replicates")

  private[server] def addMember(member: Member) = if (!membersList.containsKey(member)) membersList.put(member.address.toString, member)
  private[server] def removeMember(member: Member) = if (membersList.containsKey(member.address.toString)) membersList.remove(member.address.toString)
  private[server] def members(): List[Member] = membersList.values().toList.sortBy(_.address.toString)
  private[server] def displayState() = {
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
    Logger("CLIENTS_WATCHER").debug(s"Cluster members are : ")
    members().foreach { member =>
      Logger("CLIENTS_WATCHER").debug(s"==> ${member.address} :: ${member.getRoles} => ${member.status}")
    }
    Logger("CLIENTS_WATCHER").debug(s"----------------------------------------------------------------------------")
  }


  def start()(implicit ec: ExecutionContext): DistributedMapNode = {
    bootSystem <== ActorSystem("UDP-Server")
    val fu = SeedHelper.bootstrapSeed(bootSystem(), config, clientOnly).map { seeds =>
      system <== ActorSystem(Env.systemName, seeds.config())
      cluster <== Cluster(system())
      service <== new DistributionService(system(), replicates)
      if (!clientOnly) {
        node <== system().actorOf(Props(classOf[NodeService], this), Env.mapService)
        db <== Iq80DBFactory.factory.open(path, options)
      }
      seeds.joinClusterIfSeed(cluster())
    }
    fu.onComplete {
      case Failure(e) => Logger.error("Something wrong happened", e)
      case _ =>
    }
    Await.result(fu, Duration(10, TimeUnit.SECONDS))
    def sync(): Unit = {
      system().scheduler.scheduleOnce(Duration(5, TimeUnit.MINUTES)) {  // TODO : from config
        Try { blockingRebalance() }
        sync()
      }
    }
    sync()
    this
  }

  def stop(): DistributedMapNode = {
    cluster().leave(cluster().selfAddress)
    bootSystem().shutdown()
    system().shutdown()
    db().close()
    this
  }

  def destroy(): Unit = {
    Iq80DBFactory.factory.destroy(path, options)
  }

  private val run = new AtomicBoolean(false)
  private[server] def rebalance(): Unit = {
    implicit val ec = system().dispatcher
    if (run.compareAndSet(false, true)) {
      system().scheduler.scheduleOnce(Duration(5, TimeUnit.SECONDS)) { // TODO : config
        blockingRebalance()
        run.compareAndSet(true, false)
      }
    }
  }

  private def blockingRebalance(): Unit = {
    import scala.collection.JavaConversions._
    implicit val ec = system().dispatcher
    val nodes = service().numberOfNodes()
    Logger.debug(s"[$name] Rebalancing to ${nodes} nodes ...")
    val keys = db().iterator().map { entry => Iq80DBFactory.asString(entry.getKey) }.toList
    Logger.debug(s"[$name] Found ${keys.size} keys")
    val rebalanced = new AtomicLong(0L)
    val filtered = keys.filter { key =>
      val target = service().target(key)
      !target.address.toString.contains(cluster().selfAddress.toString)
    }
    Logger.debug(s"[$name] Should move ${filtered.size} keys")
    filtered.map { key =>
      val doc = getOperation(GetOperation(key, System.currentTimeMillis(), generator.nextId()))
      deleteOperation(DeleteOperation(key, System.currentTimeMillis(), generator.nextId())) // Hot !!!
      // TODO : not safe operation here. What if None, or retry
      Await.result(set(key, doc.value.getOrElse(Json.obj())), Duration(10, TimeUnit.SECONDS))  // TODO : config
      rebalanced.incrementAndGet()
    }
    Logger.debug(s"[$name] Rebalancing $nodes nodes done ! ${rebalanced.get()} key moved")
  }

  private[server] def setOperation(op: SetOperation): OpStatus = {
    counterWrite.incrementAndGet()
    Try { db().put(Iq80DBFactory.bytes(op.key), Iq80DBFactory.bytes(Json.stringify(op.value))) } match {
      case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId)
      case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId)
    }
  }

  private[server] def deleteOperation(op: DeleteOperation): OpStatus = {
    counterDelete.incrementAndGet()
    Try { db().delete(Iq80DBFactory.bytes(op.key)) } match {
      case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId)
      case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId)
    }
  }

  private[server] def getOperation(op: GetOperation): OpStatus = {
    counterRead.incrementAndGet()
    val opt = Option(Iq80DBFactory.asString(db().get(Iq80DBFactory.bytes(op.key)))).map(Json.parse)
    OpStatus(true, op.key, opt, op.timestamp, op.operationId)
  }

  def displayStats(): DistributedMapNode = {
    Logger.info(s"[$name] read ops ${counterRead.get()} / write ops ${counterWrite.get()} / delete ops ${counterDelete.get()}")
    this
  }

  def targets(key: String): Seq[Address] = service().targetAndNext(key).map(_.address)

  // Client API

  def set(key: String, value: JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val targets = service().targetAndNext(key)
    service().performAndWaitForQuorum(SetOperation(key, value, System.currentTimeMillis(), generator.nextId()), targets)
  }

  def set(key: String)(value: => JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val targets = service().targetAndNext(key)
    service().performAndWaitForQuorum(SetOperation(key, value, System.currentTimeMillis(), generator.nextId()), targets)
  }

  def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val targets = service().targetAndNext(key)
    service().performAndWaitForQuorum(DeleteOperation(key, System.currentTimeMillis(), generator.nextId()), targets)
  }

  def get(key: String)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
    val targets = service().targetAndNext(key)
    service().performAndWaitForQuorum(GetOperation(key, System.currentTimeMillis(), generator.nextId()), targets).map(_.value)
  }
}

object DistributedMapNode {
  def apply() = new DistributedMapNode(IdGenerator.uuid, 2, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid))
  def apply(replicates: Int) = new DistributedMapNode(IdGenerator.uuid, replicates, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid))
  def apply(name: String, replicates: Int) = new DistributedMapNode(name, replicates, new Configuration(ConfigFactory.load()), new File(name))
  def apply(name: String) = new DistributedMapNode(name, 2, new Configuration(ConfigFactory.load()), new File(name))
  def apply(replicates: Int, path: File) = new DistributedMapNode(IdGenerator.uuid, replicates, new Configuration(ConfigFactory.load()), path)
  def apply(name: String, replicates: Int, path: File) = new DistributedMapNode(name, replicates, new Configuration(ConfigFactory.load()), path)
  def apply(replicates: Int, config: Configuration, path: File) = new DistributedMapNode(IdGenerator.uuid, replicates, config, path)
  def apply(replicates: Int, config: Config, path: File) = new DistributedMapNode(IdGenerator.uuid, replicates, new Configuration(config), path)
  def apply(name: String, replicates: Int, config: Configuration, path: File) = new DistributedMapNode(name, replicates, config, path)
  def apply(name: String, replicates: Int, config: Config, path: File) = new DistributedMapNode(name, replicates, new Configuration(config), path)
}
