package com.datastax.driver.mapping;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

/**
 * This is an implementation of {@link AuditLogger} that stores audit
 * events in Cassandra tables.
 */
public class CassandraAuditLogger implements AuditLogger {

	private static CassandraAuditLogger INSTANCE;
	
	private final Session session;
	private volatile Map<String, PreparedStatement> preparedQueries = new HashMap<String, PreparedStatement>();
	private volatile Map<String, List<String>> primaryKeyColumns = new HashMap<String, List<String>>();
	
	public static class AuditRow {
		static String COL_TIMESTAMP = "time";
		static String COL_EXEC_TIME = "exec";
		static String COL_ERROR = "err";
		static String COL_MUTATION_TYPE = "type";
		static String COL_CQL_STRING = "cql";
		static String COL_STATEMENT_VALUES = "values";
		
		static String INSERT_MUTATION = "INSERT";
		static String UPDATE_MUTATION = "UPDATE";
		static String DELETE_MUTATION = "DELETE";
		static String UKNOWN_MUTATION = "UNKNOWN";
	}
	
	/**
	 * Returns the singleton instance.
	 * 
	 * @param session Cassandra session used to save audit events
	 * @return {@link CassandraAuditLogger} singleton instance
	 */
	public static CassandraAuditLogger getInstance(Session session) {
		if (INSTANCE == null) {
			INSTANCE = new CassandraAuditLogger(session);
		}
		return INSTANCE;
	}	
	
	/**
	 * Constructs {@link CassandraAuditLogger} instance using the given 
	 * session for storing audit events.
	 * 
	 * @param session Cassandra session
	 */
	public CassandraAuditLogger(Session session) {
		this.session = session;
	}
	
	public <T> void init(AuditMapper<T> mapper) {
		// audited entity is identified by its keyspace and table name
		String entityName = trim(mapper.mapper.getKeyspace()) + "." + trim(mapper.mapper.getTable());

		// if the logger is already initialized for this event,
		// silently ignore 
		if (preparedQueries.containsKey(entityName)) {
			return;
		}
		
		// create audit table and wait till schema change is propagated
		String keyspace = mapper.auditOptions.keyspaceName;
		String table = mapper.auditOptions.tableName;
		session.execute(createAuditTable(keyspace, table, mapper.mapper))
			.getExecutionInfo().isSchemaInAgreement();

		// save the entity's primary key column names
		synchronized (primaryKeyColumns) {
			primaryKeyColumns.put(entityName, getKeyColumns(mapper.mapper));
		}
		
		// prepare statement for inserting audit events
		PreparedStatement stmt = session.prepare(
				makePreparedStatement(keyspace, table, mapper.mapper));
		synchronized (preparedQueries) {
			preparedQueries.put(entityName, stmt);
		}
		
	}
	
	/* (non-Javadoc)
	 * @see io.smartcat.cassandra_audit.AuditLogger#log(com.datastax.driver.core.Statement, com.datastax.driver.mapping.Mapper)
	 */
	@Override
	public void log(long execTime, String error, BoundStatement origStatement) {
		PreparedStatement origPreparedStatement = origStatement.preparedStatement();
		
		String entityName = trim(origPreparedStatement.getVariables().getKeyspace(0)) + "." + 
				trim(origPreparedStatement.getVariables().getTable(0));

		PreparedStatement ps = preparedQueries.get(entityName);
		if (ps == null) {
			throw new IllegalStateException("AuditLogger has not been initilized for " + entityName);
		}
		
		BoundStatement bs = ps.bind();
		
		List<String> pkc = primaryKeyColumns.get(entityName);
		StringBuffer values = new StringBuffer();
		ColumnDefinitions columns = origPreparedStatement.getVariables();
    	for (Definition def : columns) { 
    		// if column is part of the entity's primary key
    		// inject it into the audit statement
    		if (pkc.contains(def.getName())) {
    			// audit key is constructed from the entity's schema so type
    			// checking is not necessary
    			bs.setBytesUnsafe(def.getName(), origStatement.getBytesUnsafe(def.getName()));
    		}
    		
    		// collect the original statemenet's values
    		values.append(def.getName());
    		values.append(":");
    		values.append(origStatement.getObject(def.getName()));
    		values.append("; ");
    	}

    	String cqlString = origPreparedStatement.getQueryString();
		bs.setDate(AuditRow.COL_TIMESTAMP, new Date());
		bs.setString(AuditRow.COL_MUTATION_TYPE, getMutationType(cqlString));
		bs.setLong(AuditRow.COL_EXEC_TIME, execTime);
		bs.setString(AuditRow.COL_ERROR, error);
		bs.setString(AuditRow.COL_CQL_STRING, cqlString);
		bs.setString(AuditRow.COL_STATEMENT_VALUES, values.toString());
		
		session.executeAsync(bs);
	}

	/**
	 * Returns mutation type based on the given CQL string.
	 * 
	 * @param cql a CQL statement string
	 * @return mutation type
	 */
	private static String getMutationType(String cql) {
		switch(cql.substring(0, 3).toUpperCase()) {
			case "INS": return AuditRow.INSERT_MUTATION;
			case "UPD": return AuditRow.UPDATE_MUTATION;
			case "DEL": return AuditRow.DELETE_MUTATION;
			default: return AuditRow.UKNOWN_MUTATION;
		}
	}
	
	/**
	 * Removes embracing double quotes chars from the given
	 * input string.
	 * 
	 * @param str an string to be trimmed 
	 * @return the trimmed value
	 */
	private static String trim(String str) {
		return str != null ? str.replaceAll("\"", "") : str;
	}
	
	/**
	 * Creates CQL statement string for inserting an audit row.
	 * 
	 * @param keyspaceName audit keyspace name
	 * @param tableName audit table name
	 * @param mapper entity's mapper
	 * @return CQL INSERT statement string
	 */
	private <T> String makePreparedStatement(String keyspaceName, String tableName, EntityMapper<T> mapper) {
		Insert insert =	insertInto(keyspaceName, tableName);

		for (ColumnMapper<T> cm : mapper.partitionKeys) {
			insert.value(cm.getColumnName(), bindMarker());
		}
		
		for (ColumnMapper<T> cm : mapper.clusteringColumns) {
			insert.value(cm.getColumnName(), bindMarker());
		}		
		
		insert.value(AuditRow.COL_TIMESTAMP, bindMarker());
		insert.value(AuditRow.COL_MUTATION_TYPE, bindMarker());
		insert.value(AuditRow.COL_EXEC_TIME, bindMarker());
		insert.value(AuditRow.COL_ERROR, bindMarker());		
		insert.value(AuditRow.COL_CQL_STRING, bindMarker());
		insert.value(AuditRow.COL_STATEMENT_VALUES, bindMarker());
		
		return insert.toString();
	}
	
	/**
	 * Returns a CQL table create statement for the designated entity auditing.
	 *  
	 * @param keyspaceName audit table keyspace
	 * @param tableName audit table name
	 * @param mapper entity's mapper
	 * @return table create statement
	 */
	private <T> Statement createAuditTable(String keyspaceName, String tableName, EntityMapper<T> mapper) {
		Create create = SchemaBuilder.createTable(keyspaceName, tableName).ifNotExists();

		for (ColumnMapper<T> cm : mapper.partitionKeys) {
			create.addPartitionKey(cm.getColumnName(), cm.getDataType());
		}
		
		for (ColumnMapper<T> cm : mapper.clusteringColumns) {
			create.addPartitionKey(cm.getColumnName(), cm.getDataType());
		}
		
		create
			.addClusteringColumn(AuditRow.COL_TIMESTAMP, DataType.timestamp())
			.addColumn(AuditRow.COL_MUTATION_TYPE, DataType.text())
			.addColumn(AuditRow.COL_EXEC_TIME, DataType.bigint())
			.addColumn(AuditRow.COL_ERROR, DataType.text())			
			.addColumn(AuditRow.COL_CQL_STRING, DataType.text())
			.addColumn(AuditRow.COL_STATEMENT_VALUES, DataType.text());
		
		return create;
	}

	/**
	 * Returns a combined list partition and clustering columns, that is, 
	 * a list of primary key columns.
	 * 
	 * @param mapper the entity's mapper
	 * @return a list of primary key columns 
	 */
	private <T> List<String> getKeyColumns(EntityMapper<T> mapper) {
		List<String> columns = new ArrayList<String>();
		
		for (ColumnMapper<T> cm : mapper.partitionKeys) {
			columns.add(trim(cm.getColumnName()));
		}
		
		for (ColumnMapper<T> cm : mapper.clusteringColumns) {
			columns.add(trim(cm.getColumnName()));
		}		
		return columns;
	}
}
