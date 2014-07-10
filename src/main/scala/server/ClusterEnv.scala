package server

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{JmxReporter, ConsoleReporter, MetricRegistry}
import config.Env

case class ClusterEnv(replicates: Int) {

  private[this] val metrics = new MetricRegistry
  private[this] val commandsTimerOut = metrics.timer("operations.out")
  private[this] val commandsTimerIn = metrics.timer("operations.in")
  private[this] val readsMeter = metrics.meter("operations.reads")
  private[this] val writesMeter = metrics.meter("operations.writes")
  private[this] val deleteMeter = metrics.meter("operations.deletes")
  private[this] val cacheSyncMeter = metrics.meter("cache.sync")
  private[this] val balanceMeter = metrics.meter("balance.sync")
  private[this] val balanceKeysMeter = metrics.meter("balance.keys")
  private[this] val quorumFailureRetryMeter = metrics.meter("quorum.failure.with.retry")
  private[this] val quorumFailureMeter = metrics.meter("quorum.failure")
  private[this] val quorumSuccessMeter = metrics.meter("quorum.success")
  //private[this] val reporter = ConsoleReporter.forRegistry(metrics).build()
  private[this] val jmxReporter = JmxReporter.forRegistry(metrics).inDomain("distributed-map").build()
  // TODO : rebalance timer
  // TODO : cache timer
  def startCommand = commandsTimerOut.time()
  def startCommandIn = commandsTimerIn.time()
  def quorumSuccess = quorumSuccessMeter.mark()
  def quorumFailure = quorumFailureMeter.mark()
  def quorumRetryFailure = quorumFailureRetryMeter.mark()
  def read = readsMeter.mark()
  def write = writesMeter.mark()
  def delete = deleteMeter.mark()
  def cacheSync = cacheSyncMeter.mark()
  def balance = balanceMeter.mark()
  def balanceKeys(n: Int) = balanceKeysMeter.mark(n)

  def start() = {
    jmxReporter.start()
    //reporter.start(10, TimeUnit.SECONDS)
  }

  def stop() = {
    jmxReporter.stop()
    //reporter.stop()
  }
}

