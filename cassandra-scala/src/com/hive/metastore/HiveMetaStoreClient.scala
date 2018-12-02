package com.hive.metastore

import org.apache.hadoop.hive.conf.HiveConf
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

object HiveMetaStoreClient extends App {

  val hiveConf = new HiveConf(getClass)
  hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://127.0.0.1:9083");
  val hiveClient = new HiveMetaStoreClient(hiveConf);

  println("No of databases :" + hiveClient.getAllDatabases().size());

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val masterSession = cluster.connect("dev")

  val statement = QueryBuilder.select().all().from("dev", "emp")
  val requestQuery = masterSession.execute(statement);

  val itr = requestQuery.all().iterator()

  while (itr.hasNext()) {

    println(itr.next().getInt("empid"))
  }

}