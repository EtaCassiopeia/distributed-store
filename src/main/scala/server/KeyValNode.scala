package server

import java.io.File
import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor._
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import common._
import config.Env
import jmx.JMXMonitor
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

class KeyValNode(val name: String, val config: Configuration, val path: File, val env: ClusterEnv, val clientOnly: Boolean = false) extends ClusterSupport with QuorumSupport with RebalanceSupport {

  private[server] val db = Reference.empty[DB]()
  private[server] val nodeService = Reference.empty[ActorRef]()
  private[server] val nodeClusterWatcher = Reference.empty[ActorRef]()
  private[server] val system = Reference.empty[ActorSystem]()
  private[server] val cluster = Reference.empty[Cluster]()
  private[server] val generator = IdGenerator(Random.nextInt(1024))
  private[server] val options = new Options()
  private[server] val running = new AtomicBoolean(false)
  private[server] val locks = new ConcurrentHashMap[String, Unit]()

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

    val clusterConfig = SeedHelper.manuallyBootstrap(address, port, config, clientOnly)
    system             <== ActorSystem(Env.systemName, clusterConfig.config)
    cluster            <== Cluster(system())
    nodeService        <== system().actorOf(Props(classOf[NodeService], this), Env.mapService)
    nodeClusterWatcher <== system().actorOf(Props(classOf[NodeClusterWatcher], this), Env.mapWatcher)

    if (!clientOnly) db <== Iq80DBFactory.factory.open(path, options)

    clusterConfig.join(cluster(), seedNodes)

    if (!clientOnly) {
      syncNode(system().dispatcher)
    }

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        syncCacheIfNecessary(true)
        stop()
      }
    }))
    this
  }

  def stop(): KeyValNode = {
    syncCacheIfNecessary(true)
    running.set(false)
    cluster().leave(cluster().selfAddress)
    system().shutdown()
    if (!clientOnly) db().close()
    this
  }

  def destroy(): Unit = {
    Iq80DBFactory.factory.destroy(path, options)
  }

  private[this] val cache = new ConcurrentHashMap[String, JsValue]()
  private[this] val cacheSetCount = new AtomicInteger(0)

  private[this] def setLevelDB(op: SetOperation): OpStatus = {
    Try { db().put(Iq80DBFactory.bytes(op.key), Iq80DBFactory.bytes(Json.stringify(op.value))) } match {
      case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId)
      case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId)
    }
  }

  private[server] def syncCacheIfNecessary(force: Boolean): Unit = {
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

  private[server] def setOperation(op: SetOperation, rollback: Boolean = false): OpStatus = {
    val ctx = env.startCommandIn
    val ctx2 = env.write
    def perform = {
      syncCacheIfNecessary(false)
      val old = Option(cache.put(op.key, op.value))
      cacheSetCount.incrementAndGet()
      ctx.close()
      ctx2.close()
      OpStatus(true, op.key, None, op.timestamp, op.operationId, old)
    }
    if (locks.containsKey(op.key)) {
      env.lock
      var attempts = 0
      while(locks.containsKey(op.key) || attempts > 50) {
        env.lockRetry
        attempts = attempts + 1
        Thread.sleep(1)
      }
    }
    if (locks.containsKey(op.key)) {
      OpStatus(false, op.key, None, op.timestamp, op.operationId)
    } else perform
  }

  private[server] def deleteOperation(op: DeleteOperation, rollback: Boolean = false): OpStatus = {
    val ctx = env.startCommandIn
    val ctx2 = env.delete
    def perform = {
      syncCacheIfNecessary(false)
      val old = Try {
        val opt1 = Option(cache.remove(op.key))
        val opt2 = Option(Iq80DBFactory.asString(db().get(Iq80DBFactory.bytes(op.key)))).map(Json.parse)
        if (opt1.isDefined) opt1 else opt2
      }.toOption.flatten
      Try {
        db().delete(Iq80DBFactory.bytes(op.key))
        ctx.close()
        ctx2.close()
      } match {
        case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId, old)
        case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId, old)
      }
    }
    if (locks.containsKey(op.key)) {
      env.lock
      var attempts = 0
      while(locks.containsKey(op.key) || attempts > 50) {
        env.lockRetry
        attempts = attempts + 1
        Thread.sleep(1)
      }
    }
    if (locks.containsKey(op.key)) {
      OpStatus(false, op.key, None, op.timestamp, op.operationId)
    } else perform
  }

  private[server] def getOperation(op: GetOperation, rollback: Boolean = false): OpStatus = {
    val ctx = env.startCommandIn
    val ctx2 = env.read
    def perform = {
      syncCacheIfNecessary(false)
      if (cache.containsKey(op.key)) {
        OpStatus(true, op.key, Option(cache.get(op.key)), op.timestamp, op.operationId)
      } else {
        val opt = Option(Iq80DBFactory.asString(db().get(Iq80DBFactory.bytes(op.key)))).map(Json.parse)
        ctx.close()
        ctx2.close()
        OpStatus(true, op.key, opt, op.timestamp, op.operationId)
      }
    }
    if (locks.containsKey(op.key)) {
      env.lock
      var attempts = 0
      while(locks.containsKey(op.key) || attempts > 50) {
        env.lockRetry
        attempts = attempts + 1
        Thread.sleep(1)
      }
    }
    if (locks.containsKey(op.key)) {
      OpStatus(false, op.key, None, op.timestamp, op.operationId)
    } else perform
  }

  // Client API
  private[server] def set(key: String, value: JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(SetOperation(key, value, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
  }

  private[server] def set(key: String)(value: => JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(SetOperation(key, value, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
  }

  private[server] def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(DeleteOperation(key, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
  }

  private[server] def get(key: String)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(GetOperation(key, time, id), targets).map(_.value)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => None     // TODO : only for managed errors
    }
  }

  private[server] def getOp(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommand
    val targets = targetAndNext(key)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(GetOperation(key, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
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
