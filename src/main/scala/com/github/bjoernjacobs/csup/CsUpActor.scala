package com.github.bjoernjacobs.csup

import akka.actor.{Actor, ActorLogging}
import akka.pattern.PipeToSupport
import com.datastax.driver.core.{Cluster, Session}

import scala.concurrent.Promise
import scala.concurrent.duration._

/**
  * @author Björn Jacobs <bjoern.jacobs@codecentric.de>
  */
class CsUpActor(val csUpConfig: CsUpConfig) extends Actor with ActorLogging with PipeToSupport {

  import context._

  val initPromise = Promise[Unit]

  override def receive: Receive = {
    case InitializationRequest =>
      val requester = sender
      become(running())
      initPromise.future.pipeTo(requester)
      self ! TryGetSession(csUpConfig.retryConnectionCount)
  }

  def running(): Receive = {
    case TryGetSession(retryCount) =>
      if (retryCount == 0) {
        val msg = "Could not connect to Cassandra and retry count was reached."
        log.error(msg)
        initPromise.failure(new RuntimeException(msg))
      }

      try {
        val clusterBuilder = Cluster.builder()
        val cluster = clusterBuilder
          .addContactPoint(csUpConfig.casConf.contactPoint)
          .withCredentials(csUpConfig.casConf.username, csUpConfig.casConf.password)
          .build()
        val session = cluster.connect()
        self ! Init(cluster, session)
      } catch {
        case e: Exception =>
          val d = csUpConfig.retryConnectionWait.toSeconds
          log.warning("Error connecting to Cassandra database", e)
          log.info(s"Retrying in $d seconds")
          context.system.scheduler.scheduleOnce(d.seconds, self, retryCount - 1)
      }

    case Init(cluster, session) =>
      try {
        if (csUpConfig.forceRecreateKeyspace) {
          log.info("Dropping keyspace if exists")
          session.execute(s"DROP KEYSPACE IF EXISTS ${csUpConfig.casConf.keyspace};")
        }

        log.info("Creating keyspace")
        val createKeyspaceStatement = csUpConfig.createKeyspaceStatement.replaceAllLiterally(csUpConfig.keyspaceNamePlaceholder, csUpConfig.casConf.keyspace)
        session.execute(createKeyspaceStatement)

        log.info("Selecting keyspace")
        session.execute(s"USE ${csUpConfig.casConf.keyspace};")

        log.info("Executing statements")
        csUpConfig.statements.foreach(stmt => {
          val stmtUpdated = stmt.replaceAllLiterally(csUpConfig.keyspaceNamePlaceholder, csUpConfig.casConf.keyspace)
          session.execute(stmtUpdated)
        })

        log.info("Closing session")
        session.close()
        cluster.close()
        unbecome()

        initPromise.success()
      } catch {
        case e: Exception => initPromise.failure(e)
      }
  }
}

private case object Start

private case class TryGetSession(x: Int)

private case class Init(cluster: Cluster, session: Session)