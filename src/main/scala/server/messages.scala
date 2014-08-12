package server

import com.google.protobuf.ByteString
import server.messages.{ResponseProto, RequestProto}
import server.messages.RequestProto.Request
import server.messages.ResponseProto.Response

trait Operation {
  val key: String
  val timestamp: Long
  val operationId: Long
  def toProtobuf: Request
}
case class Coordinates(operation: Operation)
case class SetOperation(key: String, value: Array[Byte], timestamp: Long, operationId: Long, start: Long = System.nanoTime()) extends Operation {
  def toProtobuf: Request = RequestProto.Request.newBuilder()
    .setOp(0)
    .setKey(key)
    .setTime(timestamp)
    .setId(operationId)
    .setStart(start)
    .setValue(ByteString.copyFrom(value))
    .build()
}
case class GetOperation(key: String, timestamp: Long, operationId: Long, start: Long = System.nanoTime()) extends Operation {
  def toProtobuf: Request = RequestProto.Request.newBuilder()
    .setOp(1)
    .setKey(key)
    .setTime(timestamp)
    .setId(operationId)
    .setStart(start)
    .clearValue()
    .build()
}
case class DeleteOperation(key: String, timestamp: Long, operationId: Long, start: Long = System.nanoTime()) extends Operation {
  def toProtobuf: Request = RequestProto.Request.newBuilder()
    .setOp(2)
    .setKey(key)
    .setTime(timestamp)
    .setId(operationId)
    .setStart(start)
    .clearValue()
    .build()
}
case class OpStatus(successful: Boolean, key: String, value: Option[Array[Byte]], timestamp: Long, operationId: Long, old: Option[Array[Byte]] = None) {
  def toProtobuf: Response = {
    var res = ResponseProto.Response.newBuilder()
      .setId(operationId)
      .setSuc(successful)
      .setKey(key)
      .setTime(timestamp)
    if (value.isDefined) {
      res = res.setValue(ByteString.copyFrom(value.get))
    } else {
      res = res.clearValue()
    }
    if (old.isDefined) {
      res = res.setOld(ByteString.copyFrom(old.get))
    } else {
      res = res.clearOld()
    }
    res.build()
  }
}
case class SyncCacheAndBalance()
case class Rollback(status: OpStatus)
case class DbClose()
case class DbDestroy()
case class DbForceSync()

