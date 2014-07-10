package jmx

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import play.api.libs.json.{JsArray, Json, JsObject}

import scala.util.{Failure, Success, Try}

object JMXMonitor {

  val mbeans = Seq(
    "distributed-map:name=balance.keys",
    "distributed-map:name=balance.sync",
    "distributed-map:name=cache.sync",
    "distributed-map:name=operations.deletes",
    "distributed-map:name=operations.in",
    "distributed-map:name=operations.out",
    "distributed-map:name=operations.reads",
    "distributed-map:name=operations.writes",
    "distributed-map:name=quorum.success",
    "distributed-map:name=quorum.failures",
    "distributed-map:name=quorum.failures.with.retry"
  )

  def data(): JsArray = {
    var obj = Json.arr()
    Try {
      val mbs = ManagementFactory.getPlatformMBeanServer
      for (mbean <- mbeans) {
        var bean = Json.obj("name" -> mbean.replace("distributed-map:name=", "").replace(".", "-"))
        val objectname = new ObjectName(mbean)
        mbs.getMBeanInfo(objectname).getAttributes.map { info =>
          mbs.getAttribute(objectname, info.getName) match {
            case a if a.getClass == classOf[String] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[String])
            case a if a.getClass == classOf[java.lang.Long] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Long])
            case a if a.getClass == classOf[java.lang.Double] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Double])
            case a if a.getClass == classOf[java.lang.Integer] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Int])
            case a if a.getClass == classOf[java.lang.Boolean] => bean = bean ++ Json.obj(info.getName -> a.asInstanceOf[Boolean])
            case a =>
          }
        }
        obj = obj :+ bean
      }
    } match {
      case Success(_) =>
      case Failure(e) =>
    }
    obj
  }
}

