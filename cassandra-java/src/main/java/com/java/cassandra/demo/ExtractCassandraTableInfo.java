package com.java.cassandra.demo;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;import com.datastax.driver.core.policies.DefaultRetryPolicy;

/**
 * This class is used extract the primarykey's information from the Cassandra
 * tables.
 * 
 * @author revanthreddy
 */

//java-cp cassandra-java.jar com.java.cassandra.demo.ExtractCassandraTableInfo --cassandraHost xxx.xx.xx --cassandraPassword xxxxxx --cassandraUserName xxxx --keyspaces demo1,demo2 --reportPath/home/centos/report

public class ExtractCassandraTableInfo implements AutoCloseable {

	private final static Logger logger = Logger.getLogger(ExtractCassandraTableInfo.class);

	private Cluster cluster;
	private String cassandraHost;
	private String cassandraUserName;
	private String cassandraPassword;
	private String reportPath;
	private String keyspaces;

	/**
	 * This method is used to initialize the Cassandra connection
	 */
	public void initConnection() {
		try {
			// Creating Cluster object
			cluster = Cluster.builder().addContactPoint(cassandraHost)
					.withCredentials(cassandraUserName, cassandraPassword).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.build();
			cluster.connect();
			System.out.println("Connected to cluster : " + cluster.getMetadata().getClusterName());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Couldn't establish Cassandra connection. caused by :" + e.getMessage(), e);
			throw new RuntimeException("Couldn't establish the connection. Failing job");
		}
	}

	private void showUsage() {
		System.out.println(
				"args: --cassandraHost <cassandraHost> --cassandraUserName <cassandraUserName> --cassandraPassword <cassandraPassword> --keyspaces <keyspaces> --reportPath <reportPath>");
		throw new RuntimeException("Invalid Input");
	}

	/**
	 * This method is used to create local directory if it doesn't exist.
	 * 
	 * @param localPath
	 */
	public void createLocalDir(String localPath) {
		File dir = new File(localPath);
		if (!dir.exists())
			logger.info("Creating local directory at location :" + localPath);
		dir.mkdir();
	}

	public ExtractCassandraTableInfo(Map<String, String> args) {
		this.cassandraHost = args.get("cassandraHost");
		this.cassandraUserName = args.get("cassandraUserName");
		this.cassandraPassword = args.get("cassandraPassword");
		this.keyspaces = args.get("keyspaces");
		this.reportPath = args.get("reportPath");

		if (cassandraHost == null || cassandraUserName == null || cassandraPassword == null || keyspaces == null
				|| reportPath == null) {
			showUsage();
		}

		// initializing the Cassandra connection
		initConnection();
	}

	/**
	 * This method is used to extract the PrimaryKeys information from each
	 * table and write it to a text file.
	 */
	public void extractTableInfo() {
		try {
			// create report directory if not exist
			createLocalDir(reportPath);
			String[] keyspacesArr = keyspaces.split(",");

			for (String keyspace : keyspacesArr) {
				Metadata metadata = cluster.getMetadata();
				Iterator<TableMetadata> tableMeta = metadata.getKeyspace(keyspace).getTables().iterator();

				while (tableMeta.hasNext()) {
					TableMetadata table = tableMeta.next();
					List<ColumnMetadata> primaryKeys = table.getPrimaryKey();

					// writing the information to a text file
					String reportFilePath = reportPath + "/" + keyspace + ".txt";
					logger.info(
							"Started writing table : " + "'" + table.getName() + "'" + " info to : " + reportFilePath);
					Utils.writeToFile("TableName : " + table.getName() + ", PrimaryKeys : " + primaryKeys,
							reportFilePath);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error in extractTableInfo. caused by :" + e.getMessage(), e);
			throw new RuntimeException("Error in extractTableInfo. Failing job");
		}
	}

	@Override
	public void close() throws Exception {
		if (this.cluster != null) {
			try {
				this.cluster.close();
			} catch (Exception e) {
				logger.error("Couldn't close cluster properly. caused by :" + e.getMessage(), e);
				throw new RuntimeException("Couldn't close cluster properly. Failing job");
			}
		}
	}

	public static void main(String[] args) {
		try (ExtractCassandraTableInfo extractJob = new ExtractCassandraTableInfo(Utils.argsParser(args))) {

			extractJob.extractTableInfo();
		} catch (Exception ex) {
			logger.error("ExtractTableInfo job failed caused by :" + ex.getCause(), ex);
			throw new RuntimeException(ex);
		}
	}
}
