package server

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.Timer.Context
import common.{Configuration, Logger}
import config.Env
import org.iq80.leveldb.Options
import org.iq80.leveldb.impl.Iq80DBFactory
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class OnDiskStore(val name: String, val config: Configuration, val path: File, val env: ClusterEnv, val clientOnly: Boolean) {

  val options = new Options()
  val db = Iq80DBFactory.factory.open(path, options)
  val cache = new ConcurrentHashMap[String, JsValue]()
  val cacheSetCount = new AtomicInteger(0)
  private[this] val locks = new ConcurrentHashMap[String, Unit]()

  options.createIfMissing(true)

  def locked(key: String) = {
    locks.containsKey(key)
  }

  def lock(key: String) = {
    locks.putIfAbsent(key, ())
  }

  def unlock(key: String) = {
    locks.remove(key)
  }

  def keys(): List[String] =  Try(db.iterator().toList.map(e => Iq80DBFactory.asString(e.getKey))).toOption.getOrElse(List())

  def close(): Unit = db.close()

  def forceSync() = syncCacheIfNecessary(true)

  def destroy(): Unit = Iq80DBFactory.factory.destroy(path, options)

  private def setLevelDB(op: SetOperation): OpStatus = {
    Try { db.put(Iq80DBFactory.bytes(op.key), Iq80DBFactory.bytes(Json.stringify(op.value))) } match {
      case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId)
      case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId)
    }
  }

  private def awaitForUnlock(key: String, perform: => OpStatus, failStatus: OpStatus, ctx: Context, ctx2: Context): OpStatus = {
    if (locked(key)) {
      env.lock
      var attempts = 0
      while(locks.containsKey(key) || attempts > 50) {
        env.lockRetry
        attempts = attempts + 1
        Thread.sleep(1)
      }
    }
    if (locked(key)) {
      ctx.close()
      ctx2.close()
      failStatus
    } else perform
  }


  private def syncCacheIfNecessary(force: Boolean): Unit = {
    if (!clientOnly)
      if (cacheSetCount.compareAndSet(Env.syncEvery, -1)) {
        val ctx = env.cacheSync
        Logger.trace(s"[$name] Sync cache with LevelDB ...")
        val batch = db.createWriteBatch()
        try {
          cache.entrySet().foreach { e =>
            //lock(e.getKey)
            batch.put(Iq80DBFactory.bytes(e.getKey), Iq80DBFactory.bytes(Json.stringify(e.getValue)))
          }
          db.write(batch)
        } finally {
          batch.close()
          //cache.entrySet().foreach { e => unlock(e.getKey) }
          cache.clear()
          ctx.close()
        }
      } else if (force) {
        val ctx = env.cacheSync
        cacheSetCount.set(0)
        Logger.info(s"[$name] Sync cache with LevelDB ...")
        val batch = db.createWriteBatch()
        try {
          cache.entrySet().foreach { e =>
            //lock(e.getKey)
            batch.put(Iq80DBFactory.bytes(e.getKey), Iq80DBFactory.bytes(Json.stringify(e.getValue)))
          }
          db.write(batch)
        } finally {
          batch.close()
          //cache.entrySet().foreach { e => unlock(e.getKey) }
          cache.clear()
          ctx.close()
        }
      }
  }

  def setOperation(op: SetOperation, rollback: Boolean = false): OpStatus = {
    val ctx = env.startCommandIn
    val ctx2 = env.write
    def perform = {
      syncCacheIfNecessary(false)
      //lock(op.key)
      val old = Option(cache.put(op.key, op.value))
      //unlock(op.key)
      cacheSetCount.incrementAndGet()
      ctx.close()
      ctx2.close()
      OpStatus(true, op.key, None, op.timestamp, op.operationId, old)
    }
    val fail = OpStatus(false, op.key, None, op.timestamp, op.operationId)
    awaitForUnlock(op.key, perform, fail, ctx, ctx2)
  }

  def deleteOperation(op: DeleteOperation, rollback: Boolean = false): OpStatus = {
    val ctx = env.startCommandIn
    val ctx2 = env.delete
    def perform = {
      syncCacheIfNecessary(false)
      //lock(op.key)
      val old = Try {
        val opt1 = Option(cache.remove(op.key))
        val opt2 = Option(Iq80DBFactory.asString(db.get(Iq80DBFactory.bytes(op.key)))).map(Json.parse)
        if (opt1.isDefined) opt1 else opt2
      }.toOption.flatten
      Try {
        db.delete(Iq80DBFactory.bytes(op.key))
        //unlock(op.key)
        ctx.close()
        ctx2.close()
      } match {
        case Success(s) => OpStatus(true, op.key, None, op.timestamp, op.operationId, old)
        case Failure(e) => OpStatus(false, op.key, None, op.timestamp, op.operationId, old)
      }
    }
    val fail = OpStatus(false, op.key, None, op.timestamp, op.operationId)
    awaitForUnlock(op.key, perform, fail, ctx, ctx2)
  }

  def getOperation(op: GetOperation, rollback: Boolean = false): OpStatus = {
    val ctx = env.startCommandIn
    val ctx2 = env.read
    def perform = {
      syncCacheIfNecessary(false)
      //lock(op.key)
      if (cache.containsKey(op.key)) {
        ctx.close()
        ctx2.close()
        val fromCache = Option(cache.get(op.key))
        //unlock(op.key)
        OpStatus(true, op.key, fromCache, op.timestamp, op.operationId)
      } else {
        val opt = Option(Iq80DBFactory.asString(db.get(Iq80DBFactory.bytes(op.key)))).map(Json.parse)
        //unlock(op.key)
        ctx.close()
        ctx2.close()
        OpStatus(true, op.key, opt, op.timestamp, op.operationId)
      }
    }
    val fail = OpStatus(false, op.key, None, op.timestamp, op.operationId)
    awaitForUnlock(op.key, perform, fail, ctx, ctx2)
  }

  def stats(): JsObject = {
    val keys: Int = this.keys().size
    val stats = Json.obj(
      "name" -> name,
      "keys" -> keys
    )
    stats
  }
}
