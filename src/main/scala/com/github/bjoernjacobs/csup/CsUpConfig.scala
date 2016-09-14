package com.github.bjoernjacobs.csup

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * @author Björn Jacobs <bjoern.jacobs@codecentric.de>
  */
case class CsUpConfig(casConf: CassandraConfig, forceRecreateKeyspace: Boolean, keyspaceNamePlaceholder: String, createKeyspaceStatement: String, statements: List[String], retryConnectionCount: Int, retryConnectionInitWait: Duration, retryConnectionWaitFactorIncrease: Double, overallInitializationTimeout: FiniteDuration) {}

case class CassandraConfig(contactPoint: String, keyspace: String, username: String, password: String)