package server

import java.io.File
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import com.google.common.hash.{HashCode, Hashing}
import com.typesafe.config.{Config, ConfigFactory}
import common._
import config.Env
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

class NodeServiceWorker(node: DistributedMapNode) extends Actor {
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => sender() ! node.getOperation(o)
    case o @ SetOperation(key, value, t, id) => sender() ! node.setOperation(o)
    case o @ DeleteOperation(key, t, id) => sender() ! node.deleteOperation(o)
    case _ =>
  }
}

class NodeService(node: DistributedMapNode) extends Actor {
  var workers = List[ActorRef]()
  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    for(i <- 0 to Env.workers) {
      workers = workers :+ context.system.actorOf(Props(classOf[NodeServiceWorker], node))
    }
  }
  def worker(key: String) = {
    val id = Hashing.consistentHash(HashCode.fromInt(key.hashCode), Env.workers)
    workers(id % Env.workers)
  }
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => worker(key) forward o
    case o @ SetOperation(key, value, t, id) => worker(key) forward o
    case o @ DeleteOperation(key, t, id) => worker(key) forward o
    case MemberUp(member) => {
      node.displayState()
      node.rebalance()
    }
    case UnreachableMember(member) => {
      node.displayState()
      node.rebalance()
    }
    case MemberRemoved(member, previousStatus) => {
      node.displayState()
      node.rebalance()
    }
    case _ =>
  }
}

class DistributedMapNode(name: String, replicates: Int = Env.minimumReplicates, config: Configuration, path: File, clientOnly: Boolean = false) extends ClusterAware {

  private[this] val db = Reference.empty[DB]()
  private[this] val node = Reference.empty[ActorRef]()
  private[this] val bootSystem = Reference.empty[ActorSystem]()
  private[this] val system = Reference.empty[ActorSystem]()
  private[this] val cluster = Reference.empty[Cluster]()
  private[this] val seeds = Reference.empty[SeedConfig]()
  private[this] val counterRead = new AtomicLong(0L)
  private[this] val counterWrite = new AtomicLong(0L)
  private[this] val counterDelete = new AtomicLong(0L)
  private[this] val counterRebalancedKey = new AtomicLong(0L)
  private[this] val generator = IdGenerator(Random.nextInt(1024))
  private[this] val options = new Options()
  private[this] val running = new AtomicBoolean(false)
  private[this] val run = new AtomicBoolean(false)

  options.createIfMissing(true)
  Logger.configure()

  if (replicates < Env.minimumReplicates) throw new RuntimeException(s"Cannot have less than ${Env.minimumReplicates} replicates")

  private[server] def replicatesNbr() = replicates
  private[server] def syncNode(ec: ExecutionContext): Unit = {
    system().scheduler.scheduleOnce(Env.autoResync) {
      Try { blockingRebalance() }
      syncNode(ec)
    }(ec)
  }

  private[server] def members(): List[Member] = {
    val md5 = Hashing.md5()
    cluster().state.getMembers.toList.sortWith { (member1, member2) =>
      val hash1 = md5.hashString(member1.address.toString, Env.UTF8).asLong()
      val hash2 = md5.hashString(member2.address.toString, Env.UTF8).asLong()
      hash1.compareTo(hash2) < 0
    }
  }

  def start()(implicit ec: ExecutionContext): DistributedMapNode = {
    running.set(true)
    bootSystem <== ActorSystem("UDP-Server")
    seeds      <== SeedHelper.bootstrapSeed(bootSystem(), config, clientOnly)
    system     <== ActorSystem(Env.systemName, seeds().config())
    cluster    <== Cluster(system())
    node       <== system().actorOf(Props(classOf[NodeService], this), Env.mapService)
    if (!clientOnly) db <== Iq80DBFactory.factory.open(path, options)
    val wait = seeds().joinCluster(cluster())
    // TODO : to wait or not to wait
    Try { Await.result(wait, Env.waitForCluster) } match {
      case Failure(e) => seeds().forceJoin()
      case _ =>
    }
    // TODO : run it when needed
    if (!clientOnly) {
      //syncNode()
    }
    this
  }

  def stop(): DistributedMapNode = {
    running.set(false)
    cluster().leave(cluster().selfAddress)
    bootSystem().shutdown()
    system().shutdown()
    seeds().shutdown()
    // TODO : wait to finish current operations ?
    if (!clientOnly) db().close()
    this
  }

  def destroy(): Unit = {
    // TODO : wait to finish current operations ?
    Iq80DBFactory.factory.destroy(path, options)
  }

  private[this] def performOperationWithQuorum(op: Operation, targets: Seq[Member]): Future[OpStatus] = {
    implicit val ec = system().dispatcher
    Future.sequence(targets.map { member =>
      Try {
        system().actorSelection(RootActorPath(member.address) / "user" / Env.mapService).ask(op)(Env.timeout).mapTo[OpStatus].recover {
          case _ => OpStatus(false, "", None, System.currentTimeMillis(), 0L)
        }
      } match {
        case Success(f) => f
        case Failure(e) => Future.successful(OpStatus(false, "", None, System.currentTimeMillis(), 0L))
      }
    }).map { fuStatuses =>
      val successfulStatuses = fuStatuses.filter(_.successful)
      successfulStatuses.headOption match {
        case Some(first) => {
          val valid = successfulStatuses.filter(_.value == first.value).map(_ => 1).fold(0)(_ + _) // TODO : handle version timestamp conflicts
          if (valid != fuStatuses.size) rebalance()
          if (valid >= quorum()) first
          else {
            Logger.error(s"Operation failed : quorum was $valid success / ${quorum()} mandatory")
            // TODO : rollback operation if no succeed ?
            // TODO : transac mode
            OpStatus(false, first.key, None, first.timestamp, first.operationId)
          }
        }
        case None => {
          // TODO : dafuq !!!
          Logger.error(s"Operation failed : no response !!!")
          OpStatus(false, "None", None, System.currentTimeMillis(), 0L)
        }
      }
    }
  }

  private[server] def rebalance(): Unit = {
    // TODO : enqueue calls ?
    implicit val ec = system().dispatcher
    if (run.compareAndSet(false, true)) {
      system().scheduler.scheduleOnce(Env.rebalanceConflate) {
        blockingRebalance()
        run.compareAndSet(true, false)
      }
    }
  }

  private[this] def blockingRebalance(): Unit = {
    if (running.get() && !clientOnly) {
      import scala.collection.JavaConversions._
      implicit val ec = system().dispatcher
      val start = System.currentTimeMillis()
      val nodes = numberOfNodes()
      Try(db().iterator().map { entry => Iq80DBFactory.asString(entry.getKey)}.toList) match {
        case Success(keys) => {
          val rebalanced = new AtomicLong(0L)
          val filtered = keys.filter { key =>
            val t = target(key)
            !t.address.toString.contains(cluster().selfAddress.toString)
          }
          Logger.debug(s"[$name] Rebalancing $nodes nodes, found ${keys.size} keys, should move ${filtered.size} keys")
          filtered.map { key =>
            getOperation(GetOperation(key, System.currentTimeMillis(), generator.nextId())).value.map { doc =>
              deleteOperation(DeleteOperation(key, System.currentTimeMillis(), generator.nextId()))
              val futureSet = Futures.retry(Env.rebalanceRetry)(set(key, doc))
              futureSet.onComplete {
                case Success(opStatus) => counterRebalancedKey.incrementAndGet()
                case Failure(e) => {
                  setOperation(SetOperation(key, doc, System.currentTimeMillis(), generator.nextId()))
                  rebalance()
                }
              }
              Await.result(futureSet, Env.waitForRebalanceKey)
              rebalanced.incrementAndGet()
            }
          }
          Logger.debug(s"[$name] Rebalancing $nodes nodes done, ${rebalanced.get()} key moved in ${System.currentTimeMillis() - start} ms.")
        }
        case _ => Logger.error("Error while accessing the node persistence unit !!!!")
      }
    }
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
    val keys: Int = Try(db().iterator().toList.size).toOption.getOrElse(-1)
    val stats = Json.obj(
      "name" -> name,
      "readOps" -> counterRead.get(),
      "writeOps" -> counterWrite.get(),
      "deleteOps" -> counterDelete.get(),
      "balanceKeys" -> counterRebalancedKey.get(),
      "keys" -> keys
    )
    Logger.info(Json.prettyPrint(stats))
    this
  }

  // Client API
  private[server] def set(key: String, value: JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val targets = targetAndNext(key)
    performOperationWithQuorum(SetOperation(key, value, System.currentTimeMillis(), generator.nextId()), targets)
  }

  private[server] def set(key: String)(value: => JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val targets = targetAndNext(key)
    performOperationWithQuorum(SetOperation(key, value, System.currentTimeMillis(), generator.nextId()), targets)
  }

  private[server] def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val targets = targetAndNext(key)
    performOperationWithQuorum(DeleteOperation(key, System.currentTimeMillis(), generator.nextId()), targets)
  }

  private[server] def get(key: String)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
    val targets = targetAndNext(key)
    performOperationWithQuorum(GetOperation(key, System.currentTimeMillis(), generator.nextId()), targets).map(_.value)
  }
}

object DistributedMapNode {
  def apply() = new DistributedMapNode(IdGenerator.uuid, Env.minimumReplicates, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid))
  def apply(replicates: Int) = new DistributedMapNode(IdGenerator.uuid, replicates, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid))
  def apply(name: String, replicates: Int) = new DistributedMapNode(name, replicates, new Configuration(ConfigFactory.load()), new File(name))
  def apply(name: String) = new DistributedMapNode(name, Env.minimumReplicates, new Configuration(ConfigFactory.load()), new File(name))
  def apply(replicates: Int, path: File) = new DistributedMapNode(IdGenerator.uuid, replicates, new Configuration(ConfigFactory.load()), path)
  def apply(name: String, replicates: Int, path: File) = new DistributedMapNode(name, replicates, new Configuration(ConfigFactory.load()), path)
  def apply(replicates: Int, config: Configuration, path: File) = new DistributedMapNode(IdGenerator.uuid, replicates, config, path)
  def apply(replicates: Int, config: Config, path: File) = new DistributedMapNode(IdGenerator.uuid, replicates, new Configuration(config), path)
  def apply(name: String, replicates: Int, config: Configuration, path: File) = new DistributedMapNode(name, replicates, config, path)
  def apply(name: String, replicates: Int, config: Config, path: File) = new DistributedMapNode(name, replicates, new Configuration(config), path)
}
