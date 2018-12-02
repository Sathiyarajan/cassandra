package com.cassandra.custom.codec;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TestCodec {

	public static void main(String[] args) {
		// Setup the cluster connection
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect("dev");

		// Configure our DateTimeCodec
		CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
		codecRegistry.register(new CustomSimpleTimestampCodec());
		try {

			// Test retrieving a DateTime value
			ResultSet results = session.execute("SELECT * FROM test WHERE id = '1'");
			System.out.println("Columns : " + results.getColumnDefinitions());
			for (Row row : results) {
				Long value = row.get("created_date", Long.class);

				System.out.println(value);
			}
		} finally {
			session.close();
			cluster.close();
		}
	}
}
