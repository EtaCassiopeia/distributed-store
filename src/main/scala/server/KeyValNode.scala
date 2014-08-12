package server

import java.io.File
import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._
import akka.cluster.Cluster
import com.google.common.hash.{HashCode, Hashing}
import com.typesafe.config.{Config, ConfigFactory}
import common._
import config.{ClusterEnv, Env}
import metrics.Metrics
import org.iq80.leveldb.impl.Iq80DBFactory
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

trait BytesReader[T] {
  def fromBytes(bytes: Array[Byte]): Try[T]
}

trait BytesWriter[T] {
  def toBytes(obj: T): Array[Byte]
}

class KeyValNode(val name: String, val config: Configuration, val path: File, val env: ClusterEnv, val metrics: Metrics, val clientOnly: Boolean = false) extends ClusterSupport with QuorumSupport with RebalanceSupport {

  private[server] val system = Reference.empty[ActorSystem]()
  private[server] val cluster = Reference.empty[Cluster]()
  private[server] val generator = IdGenerator(Random.nextInt(1024))

  private[server] val dbs = if (clientOnly) List[OnDiskStore]() else for (i <- 0 to Env.cells) yield new OnDiskStore(s"$name-cell-$i", config, new File(path, s"cell-$i"), env, metrics, clientOnly)
  private[server] val running = new AtomicBoolean(false)

  Logger.configure()


  private[server] def lock(key: String) = {
    NodeCell.cellDb(key, this).lock(key)
  }

  private[server] def unlock(key: String) = {
    NodeCell.cellDb(key, this).unlock(key)
  }

  private[server] def syncNode(ec: ExecutionContext): Unit = {
    Try {
      system().scheduler.scheduleOnce(Env.autoResync) {
        rebalance()
        syncNode(ec)
      }(ec)
    }
  }

  def start(address: String = InetAddress.getLocalHost.getHostAddress, port: Int = SeedHelper.freePort, seedNodes: Seq[String] = Seq())(implicit ec: ExecutionContext): KeyValNode = {

    running.set(true)

    val clusterConfig = SeedHelper.manualBootstrap(address, port, config, clientOnly)
    system             <== ActorSystem(Env.systemName, clusterConfig.config)
    cluster            <== Cluster(system())

    if (!clientOnly) {
      for (i <- 0 to Env.cells) {
        system().actorOf(Props(classOf[NodeCell], NodeCell.formattedName(i), dbs(i), metrics).withMailbox("map-config.akka.cell-prio-mailbox"), NodeCell.formattedName(i))
      }
    }
    if (!clientOnly) system().actorOf(Props(classOf[NodeClusterWatcher], this), Env.mapWatcher)

    clusterConfig.join(cluster(), seedNodes)

    if (!clientOnly) syncNode(system().dispatcher)

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        stop()
      }
    }))
    this
  }

  def stop(): KeyValNode = {
    for (i <- 0 to Env.cells) {
      system().actorSelection(NodeCell.formattedPath(i)) ! DbForceSync()
    }
    running.set(false)
    cluster().leave(cluster().selfAddress)
    system().shutdown()
    if (!clientOnly) {
      for (i <- 0 to Env.cells) {
        system().actorSelection(NodeCell.formattedPath(i)) ! DbClose()
      }
    }
    this
  }

  def destroy(): Unit = {
    for (i <- 0 to Env.cells) {
      system().actorSelection(NodeCell.formattedPath(i)) ! DbDestroy()
    }
  }

  // Internal API
  private[server] def internalSetBytes(key: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = metrics.startCommand
    val targets = targetAndNext(key, env.replicates)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(SetOperation(key, value, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
  }

  private[server] def internaGetBytes(key: String)(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = {
    val ctx = metrics.startCommand
    val targets = targetAndNext(key, env.replicates)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(GetOperation(key, time, id), targets).map(_.value)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => None     // TODO : only for managed errors
    }
  }

  private[server] def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = metrics.startCommand
    val targets = targetAndNext(key, env.replicates)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(DeleteOperation(key, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
  }

  // Client API

  private[server] def set[T](key: String, value: T)(implicit w: BytesWriter[T], ec: ExecutionContext): Future[OpStatus] = {
    internalSetBytes(key, w.toBytes(value))(ec)
  }

  private[server] def get[T](key: String)(implicit r: BytesReader[T], ec: ExecutionContext): Future[Option[T]] = {
    internaGetBytes(key)(ec).map(_.flatMap(r.fromBytes(_).toOption))
  }

  private[server] def setString(key: String, value: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    internalSetBytes(key, Iq80DBFactory.bytes(value))(ec)
  }

  private[server] def getString(key: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    internaGetBytes(key)(ec).map(_.map(Iq80DBFactory.asString(_)))
  }

  private[server] def setBytes(key: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[OpStatus] = {
    internalSetBytes(key, value)(ec)
  }

  private[server] def getBytes(key: String)(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = {
    internaGetBytes(key)(ec)
  }

  private[server] def getOp(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = metrics.startCommand
    val targets = targetAndNext(key, env.replicates)
    val time = System.currentTimeMillis()
    val id = generator.nextId()
    performOperationWithQuorum(GetOperation(key, time, id), targets)
      .andThen { case _ => ctx.close() }
      .recover {
      case _ => OpStatus(false, key, None, time, id)   // TODO : only for managed errors
    }
  }

  def displayStats(): KeyValNode = {
    //Logger.info(Json.prettyPrint(db.stats()))
    this
  }
}

object KeyValNode {
  def apply(env: ClusterEnv) = new KeyValNode(IdGenerator.uuid, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid), env, env.metrics)
  def apply(name: String, env: ClusterEnv) = new KeyValNode(name, new Configuration(ConfigFactory.load()), new File(name), env, env.metrics)
  def apply(name: String, env: ClusterEnv, path: File) = new KeyValNode(name, new Configuration(ConfigFactory.load()), path, env, env.metrics)
  def apply(name: String, env: ClusterEnv, config: Configuration, path: File) = new KeyValNode(name, config, path, env, env.metrics)
  def apply(name: String, env: ClusterEnv, config: Config, path: File) = new KeyValNode(name, new Configuration(config), path, env, env.metrics)
}
