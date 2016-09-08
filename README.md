# csup
csup (Cassandra Up) performs initialization of a Cassandra database given a contact-point, optional credentials, a keyspace name to be created and cql scripts for table creation. The initialization is performed async in a Future and completes with the status of the initalization.

The main use-case of this is when you have an application in a Docker container that is linked with a Cassandra container. When you start-up the containers using e.g. docker-compose, the csup-tool will retry to connect to Cassandra until connection succeeds. Then it will perform the initialization procedure. When the initialization completes, your main application will be notified by the completion of the Future and you can start it up with a ready-to-use Cassandra database.

## How to use
Configure your environment using your own `application.conf` and `cassandra_init_statements.conf.

```
package com.mypackage

import scala.util.{Failure, Success}
import com.github.bjoernjacobs.csup.CsUp`

object MyApp extends App {
  implicit val ec = scala.concurrent.ExecutionContext.global`
  
  CsUp().init().onComplete({
    case Success(_) => startUp() // start up your application
    case Failure(ex) => ex.printStackTrace()
  })
}
```
