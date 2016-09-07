package com.github.bjoernjacobs.csup

import scala.concurrent.duration.Duration

/**
  * @author Björn Jacobs <bjoern.jacobs@codecentric.de>
  */
case class CsUpConfig(casConf: CassandraConfig, forceRecreateKeyspace: Boolean, keyspaceNamePlaceholder: String, createKeyspaceStatement: String, statements: List[String], retryConnectionCount: Int, retryConnectionWait: Duration) {}

case class CassandraConfig(contactPoint: String, keyspace: String, username: String, password: String)