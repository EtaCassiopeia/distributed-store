package server

import java.io.File

import com.typesafe.config.ConfigFactory
import common.{Configuration, IdGenerator}
import play.api.libs.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

class NodeClient(env: ClusterEnv, node: KeyValNode)  {

  def start(seedNodes: Seq[String])(implicit ec: ExecutionContext): NodeClient = {
    node.start(seedNodes = seedNodes)(ec)
    this
  }

  def stop(): NodeClient = {
    node.stop()
    this
  }

  def set(key: String, value: JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.set(key, value)(ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

  def set(key: String)(value: => JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.set(key, value)(ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

  def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.delete(key)(ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

  def get(key: String)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
    val ctx = env.startCommandclient
    node.get(key)(ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

}

object NodeClient {
  def apply(env: ClusterEnv) = new NodeClient(env, new KeyValNode(IdGenerator.uuid, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid), env, true))
}
