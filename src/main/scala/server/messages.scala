package server

import play.api.libs.json.JsValue

trait Operation
case class SetOperation(key: String, value: JsValue, timestamp: Long, operationId: Long) extends Operation
case class GetOperation(key: String, timestamp: Long, operationId: Long) extends Operation
case class DeleteOperation(key: String, timestamp: Long, operationId: Long) extends Operation
case class OpStatus(successful: Boolean, key: String, value: Option[JsValue], timestamp: Long, operationId: Long)