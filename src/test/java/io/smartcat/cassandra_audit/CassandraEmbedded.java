package io.smartcat.cassandra_audit;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraEmbedded {

    private static String contactPoints = "127.0.0.1";
    private static int port = 9663;
    private static String keyspace = "cassandraaudit";
    
	private static Session session;
	
	public CassandraEmbedded() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		try {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();
        session = cluster.connect();
        final CQLDataLoader cqlDataLoader = new CQLDataLoader(session);
        cqlDataLoader.load(new ClassPathCQLDataSet("db.cql", false, true, keyspace));
	}

}
