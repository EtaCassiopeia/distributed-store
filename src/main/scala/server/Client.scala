package server

import java.io.File

import com.typesafe.config.ConfigFactory
import common.{Configuration, IdGenerator}
import config.ClusterEnv
import metrics.Metrics
import play.api.libs.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

class NodeClient(env: Metrics, node: KeyValNode)  {

  def start(seedNodes: Seq[String])(implicit ec: ExecutionContext): NodeClient = {
    node.start(seedNodes = seedNodes)(ec)
    this
  }

  def stop(): NodeClient = {
    node.stop().destroy()
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

  def getOp(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.getOp(key)(ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

}

object NodeClient {
  def apply(env: ClusterEnv) = new NodeClient(env.metrics, new KeyValNode(s"client-${IdGenerator.token(10)}", new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid), env, env.metrics, true))
}
