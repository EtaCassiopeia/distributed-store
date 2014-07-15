package server

import java.io.File
import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import common._
import config.Env
import org.iq80.leveldb.impl.Iq80DBFactory
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

class KeyValNode(val name: String, val config: Configuration, val path: File, val env: ClusterEnv, val clientOnly: Boolean = false) extends ClusterSupport with QuorumSupport with RebalanceSupport {

  private[server] val db = new OnDiskStore(name, config, path, env, clientOnly)
  private[server] val system = Reference.empty[ActorSystem]()
  private[server] val cluster = Reference.empty[Cluster]()
  private[server] val generator = IdGenerator(Random.nextInt(1024))

  private[server] val running = new AtomicBoolean(false)

  Logger.configure()

  private[server] def replicatesNbr() = env.replicates

  private[server] def lock(key: String) = db.lock(key)

  private[server] def unlock(key: String) = db.unlock(key)

  private[server] def syncNode(ec: ExecutionContext): Unit = {
    Try {
      system().scheduler.scheduleOnce(Env.autoResync) {
        Try { blockingRebalance() }
        syncNode(ec)
      }(ec)
    }
  }

  def start(address: String = InetAddress.getLocalHost.getHostAddress, port: Int = SeedHelper.freePort, seedNodes: Seq[String] = Seq())(implicit ec: ExecutionContext): KeyValNode = {

    running.set(true)

    val clusterConfig = SeedHelper.manuallyBootstrap(address, port, config, clientOnly)
    system             <== ActorSystem(Env.systemName, clusterConfig.config)
    cluster            <== Cluster(system())

    system().actorOf(Props(classOf[NodeService], this), Env.mapService)
    system().actorOf(Props(classOf[NodeClusterWatcher], this), Env.mapWatcher)

    clusterConfig.join(cluster(), seedNodes)

    if (!clientOnly) syncNode(system().dispatcher)

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        db.forceSync()
        stop()
      }
    }))
    this
  }

  def stop(): KeyValNode = {
    db.forceSync()
    running.set(false)
    cluster().leave(cluster().selfAddress)
    system().shutdown()
    if (!clientOnly) db.close()
    this
  }

  def destroy(): Unit = {
    db.destroy()
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
    Logger.info(Json.prettyPrint(db.stats()))
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
