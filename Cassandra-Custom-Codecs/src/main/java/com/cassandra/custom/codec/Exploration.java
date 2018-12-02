package com.cassandra.custom.codec;

import org.joda.time.DateTime;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Exploration {
	public static void main(String[] args) {
		// Setup the cluster connection
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect("dev");

		// Configure our DateTimeCodec
		CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
		codecRegistry.register(new DateTimeCodec());
		try {
			// Test writing a DateTime value
			session.execute("INSERT INTO dev.my_table (partition_key, some_timestamp) VALUES (?, ?)", "foo",
					DateTime.now());

			// Test retrieving a DateTime value
			ResultSet results = session.execute("SELECT * FROM dev.my_table WHERE partition_key = 'foo'");
			System.out.println("Columns : " + results.getColumnDefinitions());
			for (Row row : results) {
				DateTime value = row.get("some_timestamp", DateTime.class);

				System.out.println(value);
				System.out.println(value.toLocalDate());
			}
		} finally {
			session.close();
			cluster.close();
		}
	}
}
