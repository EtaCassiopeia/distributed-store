package server

import java.io.File
import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import com.google.common.hash.{HashCode, Hashing}
import com.typesafe.config.{Config, ConfigFactory}
import common._
import common.flatfutures.sequenceOfFuture
import config.Env
import jmx.JMXMonitor
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

class NodeServiceWorker(node: KeyValNode) extends Actor {
  override def receive: Receive = {
    case o @ GetOperation(key, t, id) => sender() ! node.getOperation(o)
    case o @ SetOperation(key, value, t, id) => sender() ! node.setOperation(o)
    case o @ DeleteOperation(key, t, id) => sender() ! node.deleteOperation(o)
    case _ =>
  }
}

class NodeService(node: KeyValNode) extends Actor {
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
    // TODO : handle ask for sync ???
    case MemberUp(member) => {
      node.updateMembers()
      node.rebalance()
    }
    case UnreachableMember(member) => {
      node.updateMembers()
      node.rebalance()
    }
    case MemberRemoved(member, previousStatus) => {
      node.updateMembers()
      node.rebalance()
    }
    case _ =>
  }
}

class KeyValNode(name: String, config: Configuration, path: File, env: ClusterEnv, clientOnly: Boolean = false) extends ClusterAware {

  private[server] val db = Reference.empty[DB]()
  private[server] val node = Reference.empty[ActorRef]()
  //private[server] val bootSystem = Reference.empty[ActorSystem]()
  private[server] val system = Reference.empty[ActorSystem]()
  private[server] val cluster = Reference.empty[Cluster]()
  //private[server] val seeds = Reference.empty[SeedConfig]()
  private[server] val generator = IdGenerator(Random.nextInt(1024))
  private[server] val options = new Options()
  private[server] val running = new AtomicBoolean(false)
  private[server] val run = new AtomicBoolean(false)

  options.createIfMissing(true)
  Logger.configure()

  private[server] def replicatesNbr() = env.replicates
  private[server] def syncNode(ec: ExecutionContext): Unit = {
    system().scheduler.scheduleOnce(Env.autoResync) {
      Try { blockingRebalance() }
      syncNode(ec)
    }(ec)
  }

  def start(address: String = InetAddress.getLocalHost.getHostAddress, port: Int = SeedHelper.freePort, seedNodes: Seq[String] = Seq())(implicit ec: ExecutionContext): KeyValNode = {
    running.set(true)
    //bootSystem <== ActorSystem("UDP-Server")
    val clusterConfig = SeedHelper.manuallyBootstrap(address, port, config, clientOnly)
    system     <== ActorSystem(Env.systemName, clusterConfig.config)
    cluster    <== Cluster(system())
    node       <== system().actorOf(Props(classOf[NodeService], this), Env.mapService)

    if (!clientOnly) db <== Iq80DBFactory.factory.open(path, options)

    clusterConfig.join(cluster(), seedNodes)

    //val wait = seeds().joinCluster(cluster())
    //seeds().
    //Try { Await.result(wait, Env.waitForCluster) } match {
    //  case Failure(e) => seeds().forceJoin()
    //  case _ =>
    //}

    // TODO : run it when needed
    if (!clientOnly) {
      //syncNode()
    }
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = stop()
    }))
    this
  }

  def stop(): KeyValNode = {
    syncCacheIfNecessary(true)
    running.set(false)
    cluster().leave(cluster().selfAddress)
    system().shutdown()
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
    def actualOperation() = {
      val ctx1 = env.startQuorumAggr
      targets.map { member =>
        Try {
          system().actorSelection(RootActorPath(member.address) / "user" / Env.mapService).ask(op)(Env.longTimeout).mapTo[OpStatus].recover {
            case _ => OpStatus(false, "", None, System.currentTimeMillis(), 0L)
          }
        } match {
          case Success(f) => f
          case Failure(e) => Future.successful(OpStatus(false, "", None, System.currentTimeMillis(), 0L))
        }
      }.asFuture.andThen {
        case _ => ctx1.close()
      }.map { fuStatuses =>
        val ctx2 = env.startQuorum
        val successfulStatuses = fuStatuses.toList.filter(_.successful).sortWith {(r1, r2) => r1.value.isDefined }
        if (successfulStatuses.size < quorum()) {
          Logger.trace(s"Operation failed : quorum was ${successfulStatuses.size} success / ${quorum()} mandatory")
          OpStatus(false, op.key, None, op.timestamp, op.operationId)
        }
        val ops = successfulStatuses.headOption match {
          case Some(first) => {
            val valid = successfulStatuses.filter(_.value == first.value).map(_ => 1).fold(0)(_ + _) // TODO : handle version timestamp conflicts
            if (valid != fuStatuses.size) rebalance()
            if (valid >= quorum()) first
            else {
              Logger.trace(s"Operation failed : quorum was $valid success / ${quorum()} mandatory")
              Logger.trace(fuStatuses.toString())
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
        ctx2.close()
        ops
      }.andThen {
        case Success(OpStatus(false, _, _, _, _)) => env.quorumFailure
        case Failure(_) => env.quorumFailure
        case Success(OpStatus(true, _, _, _, _)) =>
      }
    }
    Futures.retryWithPredicate[OpStatus](10, _.successful)(actualOperation()).andThen {
      case Success(OpStatus(false, _, _, _, _)) => env.quorumRetryFailure
      case Success(OpStatus(true, _, _, _, _)) => env.quorumSuccess
      case Failure(_) => env.quorumRetryFailure
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
      val ctx = env.balance
      implicit val ec = system().dispatcher
      val start = System.currentTimeMillis()
      val nodes = numberOfNodes()
      Try(db().iterator().map { entry => Iq80DBFactory.asString(entry.getKey)}.toList) match {
        case Success(keys) => {
          val rebalanced = new AtomicInteger(0)
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
                case Success(opStatus) => //counterRebalancedKey.incrementAndGet()
                case Failure(e) => {
                  setOperation(SetOperation(key, doc, System.currentTimeMillis(), generator.nextId()))
                  rebalance()
                }
              }
              Await.result(futureSet, Env.waitForRebalanceKey)
              rebalanced.incrementAndGet()
            }
          }
          env.balanceKeys(rebalanced.get())
          Logger.debug(s"[$name] Rebalancing $nodes nodes done, ${rebalanced.get()} key moved in ${System.currentTimeMillis() - start} ms.")
        }
        case _ => Logger.error("Error while accessing the node persistence unit !!!!")
      }
      ctx.close()
    }
  }

  private[this] val cache = new ConcurrentHashMap[String, JsValue]()
  private[this] val cacheSetCount = new AtomicInteger(0)
  private[this] val synchScheduled = new AtomicBoolean(false)
  private[this] def setLevelDB(op: SetOperation): OpStatus = {
    Try { db().put(Iq80DBFactory.bytes(op.key), Iq80DBFactory.bytes(Json.stringify(op.value))) } match {
      case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId)
      case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId)
    }
  }
  private[this] def syncCacheIfNecessary(force: Boolean): Unit = {
    if (!clientOnly)
      if (cacheSetCount.compareAndSet(Env.syncEvery, -1)) {
      val ctx = env.cacheSync
      Logger.trace(s"[$name] Sync cache with LevelDB ...")
      val batch = db().createWriteBatch()
      try {
        cache.entrySet().foreach(e => batch.put(Iq80DBFactory.bytes(e.getKey), Iq80DBFactory.bytes(Json.stringify(e.getValue))))
        db().write(batch)
      } finally {
        batch.close()
        cache.clear()
        ctx.close()
      }
      // TODO : not safe if crash ....
      //if (synchScheduled.compareAndSet(false, true)) {
      //  Try { system().scheduler.scheduleOnce(Duration(30, TimeUnit.SECONDS)) {
      //      Try { syncCacheIfNecessary(true) }
      //      synchScheduled.compareAndSet(true, false)
      //    }(system().dispatcher)
      //  }
      //}
    } else if (force) {
      val ctx = env.cacheSync
      cacheSetCount.set(0)
      Logger.info(s"[$name] Sync cache with LevelDB ...")
      val batch = db().createWriteBatch()
      try {
        cache.entrySet().foreach(e => batch.put(Iq80DBFactory.bytes(e.getKey), Iq80DBFactory.bytes(Json.stringify(e.getValue))))
        db().write(batch)
      } finally {
        batch.close()
        cache.clear()
        ctx.close()
      }
    }
  }

  private[server] def setOperation(op: SetOperation): OpStatus = {
    val ctx = env.startCommandIn
    syncCacheIfNecessary(false)
    env.write
    cache.put(op.key, op.value)
    cacheSetCount.incrementAndGet()
    ctx.close()
    OpStatus(true, op.key, None, op.timestamp, op.operationId)
  }

  private[server] def deleteOperation(op: DeleteOperation): OpStatus = {
    val ctx = env.startCommandIn
    syncCacheIfNecessary(false)
    env.delete
    Try {
      cache.remove(op.key)
      db().delete(Iq80DBFactory.bytes(op.key))
      ctx.close()
    } match {
      case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId)
      case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId)
    }
  }

  private[server] def getOperation(op: GetOperation): OpStatus = {
    val ctx = env.startCommandIn
    syncCacheIfNecessary(false)
    env.read
    if (cache.containsKey(op.key)) {
      OpStatus(true, op.key, Option(cache.get(op.key)), op.timestamp, op.operationId)
    } else {
      val opt = Option(Iq80DBFactory.asString(db().get(Iq80DBFactory.bytes(op.key)))).map(Json.parse)
      ctx.close()
      OpStatus(true, op.key, opt, op.timestamp, op.operationId)
    }
  }

  // Client API
  // TODO : coordinates node ???

  private[server] def set(key: String, value: JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val fu = performOperationWithQuorum(SetOperation(key, value, System.currentTimeMillis(), generator.nextId()), targets)
    fu.onComplete { case _ => ctx.close() }
    fu
  }

  private[server] def set(key: String)(value: => JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val fu = performOperationWithQuorum(SetOperation(key, value, System.currentTimeMillis(), generator.nextId()), targets)
    fu.onComplete { case _ => ctx.close() }
    fu
  }

  private[server] def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val fu = performOperationWithQuorum(DeleteOperation(key, System.currentTimeMillis(), generator.nextId()), targets)
    fu.onComplete { case _ => ctx.close() }
    fu
  }

  private[server] def get(key: String)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val fu = performOperationWithQuorum(GetOperation(key, System.currentTimeMillis(), generator.nextId()), targets).map(_.value)
    fu.onComplete { case _ => ctx.close() }
    fu
  }

  def displayStats(): KeyValNode = {
    val keys: Int = Try(db().iterator().toList.size).toOption.getOrElse(-1)
    val stats = Json.obj(
      "name" -> name,
      "keys" -> keys
    )
    Logger.info(Json.prettyPrint(stats ++ Json.obj("probes" -> JMXMonitor.data())))
    this
  }
}

object KeyValNode {
  def apply(env: ClusterEnv) = new KeyValNode(IdGenerator.uuid, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid), env)
  def apply(name: String, env: ClusterEnv) = new KeyValNode(name, new Configuration(ConfigFactory.load()), new File(name), env)
  def apply(name: String, env: ClusterEnv, path: File) = new KeyValNode(name, new Configuration(ConfigFactory.load()), path, env)
  def apply(name: String, env: ClusterEnv, config: Configuration, path: File) = new KeyValNode(name, config, path, env)
  def apply(name: String, env: ClusterEnv, config: Config, path: File) = new KeyValNode(name, new Configuration(config), path, env)
}
