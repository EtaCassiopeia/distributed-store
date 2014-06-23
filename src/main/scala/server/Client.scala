package server

import java.io.File

import com.typesafe.config.ConfigFactory
import common.{Configuration, IdGenerator}
import config.Env
import play.api.libs.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

class NodeClient(node: DistributedMapNode)  {

  def start()(implicit ec: ExecutionContext): NodeClient = {
    node.start()(ec)
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
  def apply() = new NodeClient(new DistributedMapNode(IdGenerator.uuid, Env.minimumReplicates, new Configuration(ConfigFactory.load()), new File(IdGenerator.uuid), true))
}
