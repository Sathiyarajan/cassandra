package com.java.cassandra.demo;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * @author revanthreddy
 *
 */
public class CassandraSecureConnection {
	private static Cluster cluster;
	private static Session session;

	public static void getSession() {
		session = cluster.connect();
	}

	public static void close() {
		session.close();
		cluster.close();
	}

	public static void connect(Config cfg) {
		List<Integer> tidList = new ArrayList<Integer>();
		try {
			String host = cfg.getProperty("cassandra.host");
			String dbName = cfg.getProperty("cassandra.masterDb.keyspace");
			String tableName = cfg.getProperty("cassandra.masterDb.table");
			String cassClusterName = cfg.getProperty("cassandra.cluster.name");
			String username = cfg.getProperty("cassandra.username");
			String password = cfg.getProperty("cassandra.password");

			cluster = Cluster.builder().addContactPoint(host).withClusterName(cassClusterName)
					.withCredentials(username, password).build();
			session = cluster.connect();

			Statement statement = QueryBuilder.select().all().from(dbName, tableName);
			ResultSet results = session.execute(statement);

			for (Row row : results) {
				tidList.add(row.getInt("empid"));
			}
			System.out.println("tidList : " + tidList);

		} catch (Exception e) {
			System.out.println("Exception in connect(): " + e.getMessage());
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public static void main(String[] args) {
		Config cfg = new Config("./src/main/resources/config.properties");
		connect(cfg);
	}

}
