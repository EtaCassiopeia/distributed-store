package config

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.util.Timeout

import scala.concurrent.duration.Duration

object Env {
  val nodeRole = "DISTRIBUTED-MAP-NODE"
  val clientRole = "DISTRIBUTED-MAP-NODE-CLIENT"
  val mapService = "DISTRIBUTED-MAP-SERVICE"
  val systemName = "distributed-map"
  val syncEvery = 2000                                      // TODO : from file
  val minimumReplicates = 2                                 // TODO : from file
  val workers = 10                                          // TODO : from file
  val UTF8 = Charset.forName("UTF-8")
  val autoResync = Duration(5, TimeUnit.MINUTES)            // TODO : from file
  val waitForCluster = Duration(5, TimeUnit.SECONDS)        // TODO : from file
  val rebalanceConflate = Duration(5, TimeUnit.SECONDS)     // TODO : from file
  val rebalanceRetry = 3                                    // TODO : from file
  val waitForRebalanceKey = Duration(10, TimeUnit.SECONDS)  // TODO : from file
  val timeout = Timeout(5, TimeUnit.SECONDS)                // TODO : from file
}
