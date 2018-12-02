package com.java.cassandra.test;

import com.java.cassandra.demo.CassandraClient;


public class ClientTest {

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect("127.0.0.1");
		client.getSession();
		client.createSchema();
		client.loadData();
		client.querySchema();
		client.closeSession();
		client.close();
	}
}
