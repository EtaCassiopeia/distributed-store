package common

import java.net._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import config.Env
import org.jgroups.{JChannel, Message, ReceiverAdapter}

import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Random, Try}

class SeedConfig(conf: Config, channel: JChannel, address: String, port: Int) {
  private[this] val joined = new AtomicBoolean(false)
  private[this] val noMore = new AtomicBoolean(false)
  private[this] val clusterRef = new AtomicReference[Cluster]()
  private[this] val addresses = new ConcurrentLinkedQueue[akka.actor.Address]()
  private[this] val waitingFor = 2
  private[this] val p = Promise[Unit]()
  def config() = conf
  def shutdown() = channel.close()
  def joinCluster(cluster: Cluster): Future[Unit] = {
    joined.set(true)
    clusterRef.set(cluster)
    addresses.offer(akka.actor.Address("akka.tcp", Env.systemName, address, port))
    p.future
  }
  private[this] def joinIfReady() = {
    if (addresses.size() == waitingFor) {
      clusterRef.get().joinSeedNodes(scala.collection.immutable.Seq().++(addresses.toSeq))
      noMore.set(true)
      p.trySuccess(())
    }
  }
  def forceJoin(): Unit = {
    clusterRef.get().joinSeedNodes(scala.collection.immutable.Seq().++(addresses.toSeq))
    noMore.set(true)
    p.trySuccess(())
  }
  private[common] def newSeed(message: String) = {
    if (joined.get() && !noMore.get()) {
      message.split("\\:").toList match {
        case addr :: prt :: Nil => {
          addresses.offer(akka.actor.Address("akka.tcp", Env.systemName, addr, prt.toInt))
          joinIfReady()
        }
      }
    }
  }
}


object SeedHelper {

  def freePort: Int = {
    Try {
      val serverSocket = new ServerSocket(0)
      val port = serverSocket.getLocalPort
      serverSocket.close()
      port
    }.toOption.getOrElse(Random.nextInt(1000) + 7000)
  }

  def bootstrapSeed(system: ActorSystem, configuration: Configuration, clientOnly: Boolean)(implicit ec: ExecutionContext): SeedConfig = {
    val configBuilder = new StringBuilder()
    var config = configuration.underlying.getConfig("map-config")
    val fallback = configuration.underlying.getConfig("map-config")
    val address = InetAddress.getLocalHost.getHostAddress
    val port = freePort
    configBuilder.append(s"akka.remote.netty.tcp.port=$port\n")
    configBuilder.append(s"akka.remote.netty.tcp.hostname=$address\n")
    config = ConfigFactory.parseString(configBuilder.toString()).withFallback(fallback)
    Logger("SeedHelper").debug(s"Akka remoting will be bound to akka.tcp://${Env.systemName}@$address:$port")
    val channel = new JChannel()
    channel.connect("distributed-map")
    val seeds = new SeedConfig(config, channel, address, port)
    channel.setReceiver(new ReceiverAdapter() {
      override def receive(msg: Message): Unit = {
        val myself = channel.getAddress
        if (msg.getSrc != myself) {
          seeds.newSeed(msg.getObject.asInstanceOf[String])
        }
      }
    })
    def broadcastWhoIAm(duration: FiniteDuration)(implicit ec: ExecutionContext): Unit = {
      Try {
        system.scheduler.scheduleOnce(duration) {
          val msg = new Message(null, channel.getAddress, s"$address:$port")
          Try {
            channel.send(msg)
            broadcastWhoIAm(Duration(2, TimeUnit.SECONDS))(ec)
          }
        }
      }
    }
    broadcastWhoIAm(Duration(1, TimeUnit.SECONDS))(ec)
    seeds
  }
}

/*
class SeedConfig(conf: Config, address: String, port: Int, server: Boolean) {
  def config() = conf
  def joinClusterIfSeed(cluster: Cluster) = if (server) {
    cluster.joinSeedNodes(Seq(akka.actor.Address("akka.tcp", Env.systemName, address, port)))
  }
}

class UDPServer(address: String, port: Int) extends Actor {
  import context.system
  Logger("UDPServer").debug(s"Running UDP server on $address:$port")
  IO(Udp) ! Udp.Bind(self, new InetSocketAddress("0.0.0.0", 6666))

  def receive = {
    case Udp.Bound(local) =>
      context.become(ready(sender()))
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      socket ! Udp.Send(ByteString(s"akka.tcp://${Env.systemName}@${address}:${port}"), remote)
    case Udp.Unbind  => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
  }
}

object SeedHelper {

  val udpPort = 6666
  val defaultRemotePort = 2550

  def bootstrapSeed(system: ActorSystem, configuration: Configuration, clientOnly: Boolean)(implicit ec: ExecutionContext): Future[SeedConfig] = {

    def openUdpServer() = {
      // Check if no other seed on the machine
      val socket = new DatagramSocket(udpPort, InetAddress.getByName("0.0.0.0"))
      socket.close()
    }

    def tryToFindSeedNode(): Future[String] = {
      def broadcast(): Future[String] = {
        Future {
          val c = new DatagramSocket()
          c.setBroadcast(true)
          c.setSoTimeout(2000)
          try {
            val sendData = "LOOKING_FOR_A_SEED".getBytes
            val sendPacket = new DatagramPacket(sendData, sendData.length, InetAddress.getByName("255.255.255.255"), udpPort)
            c.send(sendPacket)
            val recvBuf = new Array[Byte](15000)
            val receivePacket = new DatagramPacket(recvBuf, recvBuf.length)
            c.receive(receivePacket)
            val message = new String(receivePacket.getData).trim()
            message
          } finally {
            c.close()
          }
        }(ec)
      }
      // 3 retries
      broadcast().fallbackTo(broadcast().fallbackTo(broadcast()))
    }
    val configBuilder = new StringBuilder()
    var config = configuration.underlying.getConfig("map-config")
    val fallback = configuration.underlying.getConfig("map-config")
    val address = InetAddress.getLocalHost.getHostAddress
    val port = Random.nextInt (1000) + 7000
    configBuilder.append(s"akka.remote.netty.tcp.port=$port\n")
    configBuilder.append(s"akka.remote.netty.tcp.hostname=$address\n")
    if (clientOnly) configBuilder.append(s"""akka.cluster.roles=["DISTRIBUTED-MAP-NODE-CLIENT"]\n""")
    Logger("SeedHelper").debug(s"Akka remoting will be bound to akka.tcp://amazing-system@$address:$port")
    val server = Try(openUdpServer()).isSuccess
    if (server) {
      system.actorOf(Props(classOf[UDPServer], address, port))
    }
    tryToFindSeedNode().map { message =>
      if (server) {
        configBuilder.append(s"""akka.cluster.seed-nodes=["$message", "akka.tcp://distributed-map@$address:$port"]""")
      } else {
        configBuilder.append(s"""akka.cluster.seed-nodes=["$message"]""")
      }
      config = ConfigFactory.parseString(configBuilder.toString()).withFallback(fallback)
      new SeedConfig(config, address, port, server)
    }(ec).recover {
      case _ => {
        if (server) {
          configBuilder.append(s"""akka.cluster.seed-nodes=["akka.tcp://distributed-map@$address:$port"]""")
        } else {
          Logger("SeedHelper").error("I'm not an UDP server but no one to contact as seed ... Dafuq ???")
        }
        config = ConfigFactory.parseString(configBuilder.toString()).withFallback(fallback)
        new SeedConfig(config, address, port, server)
      }
    }(ec)
  }
}
*/

