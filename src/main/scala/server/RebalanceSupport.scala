package server

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import common.{Futures, Logger}
import config.Env

import scala.concurrent.Await
import scala.util.{Failure, Success}

trait RebalanceSupport { self: KeyValNode =>

  private[server] val rebalanceRun = new AtomicBoolean(false)
  private[server] val rebalanceRunAgain = new AtomicBoolean(false)

  private[server] def rebalance(): Unit = {
    implicit val ec = system().dispatcher
    if (running.get() && rebalanceRun.compareAndSet(false, true)) {
      system().scheduler.scheduleOnce(Env.rebalanceConflate) {
        blockingRebalance()
        rebalanceRun.compareAndSet(true, false)
        if (rebalanceRunAgain.get()) {
          rebalanceRunAgain.compareAndSet(true, false)
          rebalance()
        }
      }
    } else {
      rebalanceRunAgain.compareAndSet(false, true)
    }
  }

  private[server] def blockingRebalance(): Unit = {
    if (running.get() && !clientOnly) {
      val ctx = metrics.balance
      implicit val ec = system().dispatcher
      val start = System.currentTimeMillis()
      val nodes = numberOfNodes()
      val keys = db.keys()
      val rebalanced = new AtomicInteger(0)
      val filtered = keys.filter { key =>
        val t = target(key)
        !t.address.toString.contains(cluster().selfAddress.toString)
      }
      Logger.debug(s"[$name] Rebalancing $nodes nodes, found ${keys.size} keys, should move ${filtered.size} keys")
      filtered.map { key =>
        db.getOperation(GetOperation(key, System.currentTimeMillis(), generator.nextId())).value.map { doc =>
          db.deleteOperation(DeleteOperation(key, System.currentTimeMillis(), generator.nextId()))
          val futureSet = Futures.retry(Env.rebalanceRetry)(set(key, doc))
          futureSet.onComplete {
            case Success(opStatus) => //counterRebalancedKey.incrementAndGet()
            case Failure(e) => {
              db.setOperation(SetOperation(key, doc, System.currentTimeMillis(), generator.nextId()))
              rebalance()
            }
          }
          Await.result(futureSet, Env.waitForRebalanceKey)
          rebalanced.incrementAndGet()
        }
      }
      metrics.balanceKeys(rebalanced.get())
      Logger.debug(s"[$name] Rebalancing $nodes nodes done, ${rebalanced.get()} key moved in ${System.currentTimeMillis() - start} ms.")
      ctx.close()
    }
  }
}
