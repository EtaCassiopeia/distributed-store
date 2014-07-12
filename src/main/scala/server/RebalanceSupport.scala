package server

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import common.{Futures, Logger}
import config.Env
import org.iq80.leveldb.impl.Iq80DBFactory

import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

trait RebalanceSupport { self: KeyValNode =>

  private[server] val run = new AtomicBoolean(false)
  private[server] val runAgain = new AtomicBoolean(false)

  private[server] def rebalance(): Unit = {
    implicit val ec = system().dispatcher
    if (run.compareAndSet(false, true)) {
      system().scheduler.scheduleOnce(Env.rebalanceConflate) {
        blockingRebalance()
        run.compareAndSet(true, false)
        if (runAgain.get()) {
          runAgain.compareAndSet(true, false)
          rebalance()
        }
      }
    } else {
      runAgain.compareAndSet(false, true)
    }
  }

  private[server] def blockingRebalance(): Unit = {
    if (running.get() && !clientOnly) {
      import scala.collection.JavaConversions._
      val ctx = env.balance
      implicit val ec = system().dispatcher
      val start = System.currentTimeMillis()
      val nodes = numberOfNodes()
      Try(db().iterator().map { entry => Iq80DBFactory.asString(entry.getKey)}.toList) match {
        case Success(keys) => {
          val rebalanced = new AtomicInteger(0)
          val filtered = keys.filter { key =>
            val t = target(key)
            !t.address.toString.contains(cluster().selfAddress.toString)
          }
          Logger.debug(s"[$name] Rebalancing $nodes nodes, found ${keys.size} keys, should move ${filtered.size} keys")
          filtered.map { key =>
            getOperation(GetOperation(key, System.currentTimeMillis(), generator.nextId())).value.map { doc =>
              deleteOperation(DeleteOperation(key, System.currentTimeMillis(), generator.nextId()))
              val futureSet = Futures.retry(Env.rebalanceRetry)(set(key, doc))
              futureSet.onComplete {
                case Success(opStatus) => //counterRebalancedKey.incrementAndGet()
                case Failure(e) => {
                  setOperation(SetOperation(key, doc, System.currentTimeMillis(), generator.nextId()))
                  rebalance()
                }
              }
              Await.result(futureSet, Env.waitForRebalanceKey)
              rebalanced.incrementAndGet()
            }
          }
          env.balanceKeys(rebalanced.get())
          Logger.debug(s"[$name] Rebalancing $nodes nodes done, ${rebalanced.get()} key moved in ${System.currentTimeMillis() - start} ms.")
        }
        case _ => Logger.error("Error while accessing the node persistence unit !!!!")
      }
      ctx.close()
    }
  }
}
