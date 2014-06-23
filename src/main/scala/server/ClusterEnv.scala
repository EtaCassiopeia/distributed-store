package server

import config.Env

case class ClusterEnv(replicates: Int = Env.minimumReplicates)

