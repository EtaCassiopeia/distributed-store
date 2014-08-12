package server

import java.io.File

import com.typesafe.config.ConfigFactory
import common.{Configuration, IdGenerator}
import config.ClusterEnv
import metrics.Metrics
import play.api.libs.json.{JsArray, JsObject, Json, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

package object implicits {
  implicit lazy val BytesAsJsValue = new BytesReader[JsValue] {
    override def fromBytes(bytes: Array[Byte]): Try[JsValue] = Try(Json.parse(bytes))
  }
  implicit lazy val JsValueAsBytes = new BytesWriter[JsValue] {
    override def toBytes(obj: JsValue): Array[Byte] = Json.stringify(obj).getBytes
  }
  implicit lazy val JsObjectAsBytes = new BytesWriter[JsObject] {
    override def toBytes(obj: JsObject): Array[Byte] = Json.stringify(obj).getBytes
  }
  implicit lazy val JsArraytAsBytes = new BytesWriter[JsArray] {
    override def toBytes(obj: JsArray): Array[Byte] = Json.stringify(obj).getBytes
  }
}

class NodeClient(env: Metrics, node: KeyValNode)  {

  def start(seedNodes: Seq[String])(implicit ec: ExecutionContext): NodeClient = {
    node.start(seedNodes = seedNodes)(ec)
    this
  }

  def stop(): NodeClient = {
    node.stop().destroy()
    this
  }

  def set[T](key: String, value: T)(implicit w: BytesWriter[T], ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.set(key, value)(w, ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

  def set[T](key: String)(value: => T)(implicit w: BytesWriter[T], ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.set(key, value)(w, ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

  def delete(key: String)(implicit ec: ExecutionContext): Future[OpStatus] = {
    val ctx = env.startCommandclient
    node.delete(key)(ec).andThen {
      case _ => ctx.close()
    }(ec)
  }

  def get[T](key: String)(implicit r: BytesReader[T], ec: ExecutionContext): Future[Option[T]] = {
    val ctx = env.startCommandclient
    node.get(key)(r, ec).andThen {
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
