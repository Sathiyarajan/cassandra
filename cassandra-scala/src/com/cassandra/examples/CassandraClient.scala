package com.cassandra.examples

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.querybuilder.QueryBuilder

object CassandraClient extends App {

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("dev")

  val statement = QueryBuilder.select().all().from("dev", "emp")
  val requestQuery = session.execute(statement);

  val itr = requestQuery.all().iterator()

  while (itr.hasNext()) {
    println("empid : " + itr.next().getInt("empid"))
  }

  cluster.close()
  session.close()

}
