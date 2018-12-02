package com.java.cassandra.demo;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * @author revanthreddy
 *
 */
public class Cassandra_CRUD {

	Cluster cluster;
	Session session;
	ResultSet results;
	Row rows;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).withRetryPolicy(DefaultRetryPolicy.INSTANCE).build();
		session = cluster.connect("dev");
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connected to cluster:" + metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.println("Datatacenter: " + host.getDatacenter() + "; Host: " + host.getAddress() + "; Rack: "
					+ host.getRack());
		}
		System.out.println("\n");
	}

	public void getSession() {
		session = cluster.connect();
	}

	public void closeSession() {
		session.close();
		cluster.close();
	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS dev WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");

		session.execute("DROP TABLE dev.users");

		session.execute("CREATE TABLE IF NOT EXISTS dev.users (" + "id int PRIMARY KEY," + "lastname text," + "age int,"
				+ "city text," + "email text," + "firstname text" + ");");
	}

	public void insertData() {
		// Insert one record into the users table

		PreparedStatement statement = session.prepare(
				"INSERT INTO dev.users" + "(id,lastname, age, city, email, firstname)" + "VALUES (?,?,?,?,?,?);");

		// BoundStatement boundStatement = new BoundStatement(statement);
		// session.execute(boundStatement.bind(1, "Jones", 35, "Austin",
		// "bob@example.com", "Bob"));
		// session.execute(boundStatement.bind(2, "Mike", 35, "CA",
		// "jane@example.com", "Seena"));

		// batch insert
		BatchStatement batchStatement = new BatchStatement();
		batchStatement.add(statement.bind(1, "Jones", 35, "Austin", "bob@example.com", "Bob"));
		batchStatement.add(statement.bind(2, "Mike", 36, "CA", "seena@example.com", "Seena"));
		batchStatement.add(statement.bind(3, "reva", 37, "India", "rev@example.com", "reddy"));
		batchStatement.add(statement.bind(4, "sha", 38, "India", "sha@example.com", "reddy"));
		batchStatement.add(statement.bind(5, "ravi", 31, "India", "ravi@example.com", "reddy"));

		session.execute(batchStatement);
	}

	public void loadData() {

		// Use select to get the user we just entered
		Statement select = QueryBuilder.select().all().from("dev", "users").where((QueryBuilder.eq("id", "1")));
		results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"), row.getInt("age"));
		}

	}

	public void updateRecord() {
		// Update the same user with a new age
		Statement update = QueryBuilder.update("dev", "users").with(QueryBuilder.set("age", 36))
				.where((QueryBuilder.eq("id", 1)));
		session.execute(update);

	}

	public void deleteRecord() {
		// Delete the user from the users table
		Statement delete = QueryBuilder.delete().from("users").where(QueryBuilder.eq("id", 1));
		results = session.execute(delete);
	}

	public void show() {
		// Show that the user is gone
		Statement select = QueryBuilder.select().all().from("dev", "users");
		results = session.execute(select);
		System.out.println(String.format("%-10s\t%-10s\t%-10s\t%-10s\n%s", "id", "firstname", "age", "city",
				"--------+--------------+-----------+-----------------"));
		for (Row row : results) {
			System.out.println(String.format("%-10s\t%-10s\t%-10s\t%-10s", row.getInt("id"),
					row.getString("firstname"), row.getInt("age"), row.getString("city")));
		}
	}

	public static void main(String[] args) {
		Cassandra_CRUD cass = new Cassandra_CRUD();
		try {
			cass.connect("127.0.0.1");
			cass.createSchema();
			cass.insertData();
			cass.updateRecord();
			cass.deleteRecord();
			cass.show();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cass.closeSession();
		}
	}

}
