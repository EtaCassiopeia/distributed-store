package config

import metrics.Metrics

object ClusterEnv {
  def apply(replicates: Int) = new ClusterEnv(replicates, (replicates / 2) + 1, (replicates / 2) + 1, new Metrics())
  def apply(replicates: Int, quorumRead: Int, quorumWrite: Int) = new ClusterEnv(replicates, quorumRead, quorumWrite, new Metrics())
}

class ClusterEnv(val replicates: Int, val quorumRead: Int, val quorumWrite: Int, val metrics: Metrics) {
  def start(name: String = "distributed-map", port: Int = 9999) = metrics.start(name, port)
  def stop() = metrics.stop()
}

