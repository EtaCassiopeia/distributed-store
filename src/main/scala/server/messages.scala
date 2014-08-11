package server

import play.api.libs.json.JsValue

trait Operation {
  val key: String
  val timestamp: Long
  val operationId: Long
}
case class Coordinates(operation: Operation)
case class SetOperation(key: String, value: JsValue, timestamp: Long, operationId: Long, start: Long = System.nanoTime()) extends Operation
case class GetOperation(key: String, timestamp: Long, operationId: Long, start: Long = System.nanoTime()) extends Operation
case class DeleteOperation(key: String, timestamp: Long, operationId: Long, start: Long = System.nanoTime()) extends Operation
case class OpStatus(successful: Boolean, key: String, value: Option[JsValue], timestamp: Long, operationId: Long, old: Option[JsValue] = None)
case class SyncCacheAndBalance()
case class Rollback(status: OpStatus)
case class DbClose()
case class DbDestroy()
case class DbForceSync()

