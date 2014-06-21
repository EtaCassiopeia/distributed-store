package common

import java.net.{DatagramPacket, DatagramSocket, InetAddress, InetSocketAddress}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.io.{IO, Udp}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import config.Env

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

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


