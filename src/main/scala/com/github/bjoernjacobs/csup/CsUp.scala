package com.github.bjoernjacobs.csup

import java.io.File

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task

import scala.collection.JavaConversions._
import scala.concurrent.duration.{Duration, FiniteDuration}
import monix.execution.Scheduler.Implicits.global

/**
  * @author Björn Jacobs <bjoern.jacobs@codecentric.de>
  */
class CsUp private(baseConfig: Option[Config] = None) extends StrictLogging {
  private val csUpConfig = loadCsUpConfig()

  def init() = initTask.timeout(csUpConfig.overallInitializationTimeout).runAsync

  def initTask = {
    logger.info("initTask")
    retryBackoff(
      acquireSession,
      csUpConfig.retryConnectionCount,
      csUpConfig.retryConnectionInitWait.asInstanceOf[FiniteDuration],
      csUpConfig.retryConnectionWaitFactorIncrease)
      .flatMap(initCluster)
  }

  def acquireSession: Task[Init] = Task {
    logger.info("acquireSession")
    val clusterBuilder = Cluster.builder()
    val cluster = clusterBuilder
      .addContactPoint(csUpConfig.casConf.contactPoint)
      .withCredentials(csUpConfig.casConf.username, csUpConfig.casConf.password)
      .build()
    val session = cluster.connect()
    Init(cluster, session)
  }

  def initCluster(init: Init): Task[Cluster] = Task {
    logger.info("initCluster")

    val (cluster, session) = Init.unapply(init).get

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

    cluster
  }

  def retryBackoff[A](source: Task[A], maxRetries: Int, firstDelay: FiniteDuration, delayIncreaseFactor: Double): Task[A] = {
    source.onErrorHandleWith {
      case ex: Exception =>
        if (maxRetries > 0) {
          logger.info(s"Retrying in $firstDelay")
          retryBackoff(source,
            maxRetries - 1,
            (firstDelay * delayIncreaseFactor).asInstanceOf[FiniteDuration],
            delayIncreaseFactor)
            .delayExecution(firstDelay)
        } else Task.raiseError(ex)
    }
  }

  case class Init(cluster: Cluster, session: Session)

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
    val retryConnectionInitWait = Duration(csUpConfig.getString("retry-connection.init-wait"))
    val retryConnectionWaitFactorIncrease = csUpConfig.getDouble("retry-connection.wait-increase-factor")

    val overallInitializationTimeout = {
      val d = Duration(csUpConfig.getString("overall-init-timeout"))
      if (!d.isFinite()) {
        fail("Overall initialization timeout must be a finite duration.")
      }
      d.asInstanceOf[FiniteDuration]
    }

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
        fail(s"Invalid value for 'type': $x")
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

    CsUpConfig(cassandraConf, forceRecreateKeyspace, keyspaceNamePlaceholder, createKeyspaceStatement, statements, retryConnectionCount, retryConnectionInitWait, retryConnectionWaitFactorIncrease, overallInitializationTimeout)
  }

  private def fail(msg: String) = {
    logger.error(msg)
    throw new IllegalArgumentException(msg)
  }
}

object CsUp {
  def apply() = new CsUp()

  def apply(baseConfig: Config) = new CsUp(Some(baseConfig))
}