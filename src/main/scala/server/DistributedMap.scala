package server

import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import com.google.common.hash.{HashCode, Hashing}
import com.typesafe.config.{Config, ConfigFactory}
import common._
import config.Env
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options, WriteOptions}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Random, Failure, Success, Try}

class DistributionService(system: ActorSystem) {

  val UTF8 = Charset.forName("UTF-8")

  def listOfNodes(): Seq[Member] = Client.members().filter(_.getRoles.contains(Env.nodeRole))

  def numberOfNodes(): Int = listOfNodes().size

  def targetAndNext(key: String): Seq[Member] = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), numberOfNodes())
    var targets = Seq[Member]()
    for (i <- 0 to Env.replicats + 1) {
      val next = (id + i) % (if (numberOfNodes() > 0) numberOfNodes() else 1)
      targets = targets :+ listOfNodes()(next)
    }
    targets
  }

  def quorum(): Int = ((Env.replicats + 1) / 2) + 1

  def performAndWaitForQuorum(op: Operation, targets: Seq[Member]): Future[OpStatus] = {
    implicit val ec = system.dispatcher
    Future.sequence(targets.map { member =>
      system.actorSelection(RootActorPath(member.address) / "user" / Env.mapService).ask(op)(Client.timeout).mapTo[OpStatus]
    }).map { fuStatuses =>
      val successfulStatuses = fuStatuses.filter(_.successful)
      val first = successfulStatuses.head
      val valid = successfulStatuses.filter(_.value == first.value).map(_ => 1).fold(0)(_ + _)
      if (valid >= quorum()) first
      else OpStatus(false, first.key, None, first.operationId, first.timestamp)
    }
  }
}

class NodeService(node: DistributedMapNode) extends Actor {
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => sender() ! node.getOperation(o)
    case o @ SetOperation(key, value, t, id) => sender() ! node.setOperation(o)
    case o @ DeleteOperation(key, t, id) => sender() ! node.deleteOperation(o)
    case _ =>
  }
}

class DistributedMapNode(name: String, config: Configuration, path: File, clientOnly: Boolean = false) {

  private val db = Reference.empty[DB]()
  private val node = Reference.empty[ActorRef]()
  private val system = Reference.empty[ActorSystem]()
  private val cluster = Reference.empty[Cluster]()
  private val service = Reference.empty[DistributionService]()
  private val counterRead = new AtomicLong(0L)
  private val counterWrite = new AtomicLong(0L)
  private val counterDelete = new AtomicLong(0L)
  private val generator = IdGenerator(Random.nextInt(1024))

  private val options = new Options()
  private val wo = new WriteOptions()

  options.createIfMissing(true)

  def start()(implicit ec: ExecutionContext): DistributedMapNode = {
    // TODO : check if sync is necessary
    // TODO : listen to cluster changes to impact consistent hashing, topology, synchro, etc ...
    val fu = SeedHelper.bootstrapSeed(config, clientOnly).map { seeds =>
      system.set(ActorSystem(Env.systemName, seeds.config()))
      Client.system.set(system())
      cluster.set(Cluster(system()))
      Client.cluster.set(cluster())
      Client.init()
      service <== new DistributionService(system())
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
    this
  }

  def stop(): DistributedMapNode = {
    system().shutdown()
    db().close()
    this
  }

  def destroy(): Unit = {
    Iq80DBFactory.factory.destroy(path, options)
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
  def apply() = new DistributedMapNode(IdGenerator.uuid, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid))
  def apply(name: String) = new DistributedMapNode(name, new Configuration(ConfigFactory.load()), new File(name))
  def apply(path: File) = new DistributedMapNode(IdGenerator.uuid, new Configuration(ConfigFactory.load()), path)
  def apply(name: String, path: File) = new DistributedMapNode(name, new Configuration(ConfigFactory.load()), path)
  def apply(config: Configuration, path: File) = new DistributedMapNode(IdGenerator.uuid, config, path)
  def apply(config: Config, path: File) = new DistributedMapNode(IdGenerator.uuid, new Configuration(config), path)
  def apply(name: String, config: Configuration, path: File) = new DistributedMapNode(name, config, path)
  def apply(name: String, config: Config, path: File) = new DistributedMapNode(name, new Configuration(config), path)
}
