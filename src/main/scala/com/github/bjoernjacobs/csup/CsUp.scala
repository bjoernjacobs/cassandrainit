package com.github.bjoernjacobs.csup

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}


/**
  * @author Björn Jacobs <bjoern.jacobs@codecentric.de>
  */
class CsUp private(baseConfig: Option[Config] = None) extends StrictLogging {
  private val csUpConfig = loadCsUpConfig()

  def init() = {
    val system = ActorSystem("csup")

    implicit val timeout = Timeout(csUpConfig.overallInitializationTimeout)
    val csUpActor = system.actorOf(Props(new CsUpActor(csUpConfig)))

    val future = (csUpActor ? InitializationRequest).mapTo[Unit]
    future.onComplete(_ => system.terminate())

    future
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

    CsUpConfig(cassandraConf, forceRecreateKeyspace, keyspaceNamePlaceholder, createKeyspaceStatement, statements, retryConnectionCount, retryConnectionWait, overallInitializationTimeout)
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

case object InitializationRequest
