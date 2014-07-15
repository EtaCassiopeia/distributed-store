package jmx

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import play.api.libs.json.{JsArray, Json}

import scala.collection.JavaConversions._
import scala.util.Try

object JMXMonitor {

  lazy val mbs = ManagementFactory.getPlatformMBeanServer

  def data(name: String): JsArray = {
    var obj = Json.arr()
    Try {
      for (objectname <- mbs.queryNames(new ObjectName(s"$name:name=*"), null).toList.sortWith { (o1, o2) => o1.getCanonicalName.compareTo(o2.getCanonicalName) < 0 }) {
        var bean = Json.obj("name" -> objectname.getCanonicalName.replace(s"$name:name=", "").replace(".", "-"))
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
    }
    obj
  }
}

