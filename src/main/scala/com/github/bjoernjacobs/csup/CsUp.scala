package com.github.bjoernjacobs.csup

import java.io.File

import com.datastax.driver.core.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration


/**
  * @author Björn Jacobs <bjoern.jacobs@codecentric.de>
  */
class CsUp private(baseConfig: Option[Config] = None) extends StrictLogging {
  private val csUpConfig = loadCsUpConfig()

  def init(): Future[Unit] = Future {
    val session = retry(
      csUpConfig.retryConnectionCount,
      csUpConfig.retryConnectionWait.toMillis,
      "Retrying to acquire session from Cassandra") {
      getSession
    }

    if (csUpConfig.forceRecreateKeyspace) {
      logger.info("Dropping keyspace if exists")
      session.execute(s"DROP KEYSPACE IF EXISTS ${csUpConfig.casConf.keyspace};")
    }

    logger.info("Creating keyspace")
    val createKeyspaceStatement = csUpConfig.createKeyspaceStatement.replaceAllLiterally(csUpConfig.keyspaceNamePlaceholder, csUpConfig.casConf.keyspace)
    session.execute(createKeyspaceStatement)

    logger.info("Selecting keyspace")
    session.execute(s"USE ${csUpConfig.casConf.keyspace};")

    logger.info("Executing statements")
    csUpConfig.statements.foreach(stmt => {
      val stmtUpdated = stmt.replaceAllLiterally(csUpConfig.keyspaceNamePlaceholder, csUpConfig.casConf.keyspace)
      session.execute(stmtUpdated)
    })

    logger.info("Closing session")
    session.close()
  }

  private def getSession = {
    val clusterBuilder = Cluster.builder()
    val cluster = clusterBuilder
      .addContactPoint(csUpConfig.casConf.contactPoint)
      .withCredentials(csUpConfig.casConf.username, csUpConfig.casConf.password)
      .build()

    cluster.connect()
  }

  def loadCsUpConfig() = {
    logger.info("Reading configuration")
    val config = baseConfig.getOrElse(ConfigFactory.load())
    val csUpConfig = config.getConfig("csup")
    val cassandraConfigKey = csUpConfig.getString("cassandra-config-key")
    val cassandraConfig = config.getConfig(cassandraConfigKey)
    val cassandraInitScriptSequenceConfig = csUpConfig.getConfig("init-script-sequence")

    // read other config items
    val forceRecreateKeyspace = csUpConfig.getBoolean("force-recreate-keyspace")
    val keyspaceNamePlaceholder = csUpConfig.getString("keyspace-name-placeholder")
    val retryConnectionCount = csUpConfig.getInt("retry-connection.count")
    val retryConnectionWait = Duration(csUpConfig.getString("retry-connection.wait"))

    // read statements
    val url = cassandraInitScriptSequenceConfig.getString("url")
    val initConfig = cassandraInitScriptSequenceConfig.getString("type") match {
      case "resource" =>
        logger.info(s"Reading init statements from resource '$url'")
        ConfigFactory.parseResources(url).getConfig("init")
      case "file" =>
        logger.info(s"Reading init statements from file '$url")
        ConfigFactory.parseFile(new File(url)).getConfig("init")
      case x =>
        val msg = s"Invalid value for 'type': $x"
        logger.error(msg)
        throw new IllegalArgumentException(msg)
    }

    val createKeyspaceStatement = initConfig.getString("create-keyspace-statement")
    val statements = initConfig.getStringList("create-statements").toList
    logger.info(s"Found one keyspace creation statement and ${statements.size} other create statements")

    logger.info("Reading Cassandra connection configuration")
    val cassandraConf = CassandraConfig(
      cassandraConfig.getString("contactpoint"),
      cassandraConfig.getString("keyspace"),
      cassandraConfig.getString("username"),
      cassandraConfig.getString("password")
    )

    CsUpConfig(cassandraConf, forceRecreateKeyspace, keyspaceNamePlaceholder, createKeyspaceStatement, statements, retryConnectionCount, retryConnectionWait)
  }

  private def retry[T](n: Int, d: Long, s: String)(code: => T): T = {
    var res: Option[T] = None
    var left = n
    while (res.isEmpty) {
      left = left - 1
      try {
        res = Some(code)
      } catch {
        case t: Throwable if left > 0 =>
          logger.info(s)
          Thread.sleep(d)
      }
    }
    res.get
  }
}

object CsUp {
  def apply() = new CsUp()

  def apply(baseConfig: Config) = new CsUp(Some(baseConfig))
}
