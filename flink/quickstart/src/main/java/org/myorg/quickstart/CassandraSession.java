package org.myorg.quickstart;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Cluster.Builder;

import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import java.util.ArrayList;

public class CassandraSession {

	private static CassandraSession csh;
	private static Session session;
	private static DataSet<Tuple4<String, String, String, String>> data;
	private static ArrayList<Tuple4<String, String, String, String>> listData;
	
	private CassandraSession() { }

	private static Session startSession() {
		Cluster cluster = null;
		Session session = null;

		try {
			cluster = Cluster.builder()
				.addContactPoint("10.0.0.10")
				.withPort(9042)
				.build();
			session = cluster.connect();
		}
		catch (Exception e) {
			System.out.println("Failed to set up Cassandra session!");
		}
		finally {
			if (cluster != null) cluster.close();
		}

		return session;
	}

	public static CassandraSession getInstance() {
		if (csh == null) {
			csh = new CassandraSession();
			session = startSession();
			listData = new ArrayList<>();
		}
		return csh;
	}

	public Session getSession() {
		return session;
	}

	public DataSet<Tuple4<String, String, String, String>> getData() {
		if (data == null) {
/*                      data = env.createInput(
                                new CassandraInputFormat<Tuple4<String, String, String, String>>(
                                        SELECT_QUERY, new ClusterBuilder() {
                                                @Override
                                                protected Cluster buildCluster(Builder builder) {
                                                        return builder.addContactPoints("127.0.0.1").build();
                                                }
                                        }
                                ), TupleTypeInfo.of(new TypeHint<Tuple4<String, String, String, String>>() {} )
                        );
*/
		}

		return data;
	}

	public ArrayList<Tuple4<String, String, String, String>> getDataAsArray() {

		final String SELECT_QUERY = "SELECT id, title, body, tags FROM final.ads_table;";

		try {
			ResultSet rs = session.execute(SELECT_QUERY);
			for (Row row : rs) {
				listData.add(new Tuple4<>(row.getString(0), row.getString(1), row.getString(2), row.getString(3)));
			}	
		}
		catch (Exception e) {
			System.out.println("Caught exception!! " + e);
		}

		return listData;
	}
}
