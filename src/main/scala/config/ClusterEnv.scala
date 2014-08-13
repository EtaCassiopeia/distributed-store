package config

import metrics.Metrics

object ClusterEnv {
  def apply(replicates: Int) = new ClusterEnv(replicates, (replicates / 2) + 1, (replicates / 2) + 1, new Metrics(), false)
  def apply(replicates: Int, fullAsync: Boolean) = new ClusterEnv(replicates, (replicates / 2) + 1, (replicates / 2) + 1, new Metrics(), fullAsync)
  def apply(replicates: Int, quorumRead: Int, quorumWrite: Int) = new ClusterEnv(replicates, quorumRead, quorumWrite, new Metrics(), false)
  def apply(replicates: Int, quorumRead: Int, quorumWrite: Int, fullAsync: Boolean) = new ClusterEnv(replicates, quorumRead, quorumWrite, new Metrics(), fullAsync)
}

class ClusterEnv(val replicates: Int, val quorumRead: Int, val quorumWrite: Int, val metrics: Metrics, val fullAsync: Boolean) {
  def start(name: String = "distributed-map", port: Int = 9999) = metrics.start(name, port)
  def stop() = metrics.stop()
}

