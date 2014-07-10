package server

import java.io.File

import com.typesafe.config.ConfigFactory
import common.{Configuration, IdGenerator}
import play.api.libs.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

class NodeClient(node: KeyValNode)  {

  def start(seedNodes: Seq[String])(implicit ec: ExecutionContext): NodeClient = {
    node.start(seedNodes = seedNodes)(ec)
    this
  }

  def stop(): NodeClient = {
    node.stop()
    this
  }

  def set(key: String, value: JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = node.set(key, value)(ec)

  def set(key: String)(value: => JsValue)(implicit ec: ExecutionContext): Future[OpStatus] = node.set(key, value)(ec)

  def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = node.delete(key)(ec)

  def get(key: String)(implicit ec: ExecutionContext): Future[Option[JsValue]] = node.get(key)(ec)

}

object NodeClient {
  def apply(env: ClusterEnv) = new NodeClient(new KeyValNode(IdGenerator.uuid, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid), env, true))
}
