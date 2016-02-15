package com.datastax.driver.mapping;

import static junit.framework.TestCase.*;

import java.util.ArrayList;
import java.util.List;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;

import io.smartcat.cassandra_audit.AuditExclusion;
import io.smartcat.cassandra_audit.AuditManager;
import io.smartcat.cassandra_audit.Auditable;
import io.smartcat.cassandra_audit.SessionProxy;

public class AuditMapperTest {

	@Rule
    public ExpectedException thrown = ExpectedException.none();
	
    private static String contactPoints = "127.0.0.1";
    private static int port = 9663;
    private static String KEYSPACE = "cassandra_audit_test";
    
	private static Session session;
	private static MappingManager manager;
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-unit.yml");
		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();
        Session nativeSession = cluster.connect();
        final CQLDataLoader cqlDataLoader = new CQLDataLoader(nativeSession);
        cqlDataLoader.load(new ClassPathCQLDataSet("db.cql", false, true, KEYSPACE));
        session = new SessionProxy(nativeSession);
        manager = AuditManager.getMappingManager(session);
	}
	
	@Before
	public void before() {
		
	}
	
	@After
	public void after() {
		
	}
	
	public List<String> tables(String keyspace) {
		List<String> tables = new ArrayList<String>();
		final Cluster cluster = session.getCluster();
		final Metadata meta = cluster.getMetadata();
		final KeyspaceMetadata keyspaceMeta = meta.getKeyspace(keyspace);
		for (final TableMetadata tableMeta : keyspaceMeta.getTables()) {
			tables.add(tableMeta.getName());
		}
		return tables;
    }
	
	public List<String> tables() {
		return tables(KEYSPACE);
	}
	
	public TableMetadata tableMetadata(String tableName, String keyspace) {
		final Cluster cluster = session.getCluster();
		final Metadata meta = cluster.getMetadata();
		final KeyspaceMetadata keyspaceMeta = meta.getKeyspace(keyspace);
		return keyspaceMeta.getTable(tableName);
	}
	
	public TableMetadata tableMetadata(String tableName) {
		return tableMetadata(tableName, KEYSPACE);
	}
	
	@Table(name="non_auditable_entity")
	public class NonAuditableEntity {
		
		@PartitionKey
		private String key;

		public NonAuditableEntity(String key) {
			super();
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}
	
	@Test
	public void test_non_auditable_entity() {		
		manager.mapper(NonAuditableEntity.class);
		assertFalse(tables().contains("audit_non_auditable_entity"));
	}
	
	@Auditable
	public class NonEntity {
		
		@PartitionKey
		private String key;

		public NonEntity(String key) {
			super();
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}
	
	@Test
	public void test_non_entity() {
		thrown.expect(IllegalArgumentException.class);
		manager.mapper(NonEntity.class);
	}
	
	@Auditable(tablePrefix = "aud_")
	@Table(name="custom_audit_table_prefix_entity")
	public class CustomAuditTablePrefixEntity {
		
		@PartitionKey
		private String key;

		public CustomAuditTablePrefixEntity(String key) {
			super();
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}	

	@Test
	public void test_custom_audit_table_prefix_entity() {		
		manager.mapper(CustomAuditTablePrefixEntity.class);
		assertTrue(tables().contains("aud_custom_audit_table_prefix_entity"));
	}
	
	@Auditable(tablePrefix = "aud_", tableName = "custom_audit_table")
	@Table(name="custom_audit_table_name_entity")
	public class CustomAuditTableNameEntity {
		
		@PartitionKey
		private String key;

		public CustomAuditTableNameEntity(String key) {
			super();
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}	

	@Test
	public void test_custom_audit_table_name_entity() {		
		manager.mapper(CustomAuditTableNameEntity.class);
		assertTrue(tables().contains("custom_audit_table"));
	}	
	
	@Auditable
	@Table(name="composite_partition_key_entity")
	public class CompositePartitionKeyEntity {
		
		@PartitionKey(0)
		private String key;

		@PartitionKey(1)
		private Integer key2;
		
		public CompositePartitionKeyEntity(String key, Integer key2) {
			this.key = key;
			this.key2 = key2;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Integer getKey2() {
			return key2;
		}

		public void setKey2(Integer key2) {
			this.key2 = key2;
		}		
	}	

	@Test
	public void test_composite_partition_key_entity() {		
		manager.mapper(CompositePartitionKeyEntity.class);
		assertTrue(tables().contains("audit_composite_partition_key_entity"));
		TableMetadata meta = tableMetadata("audit_composite_partition_key_entity");
		assertEquals(2, meta.getPartitionKey().size());
		assertEquals("key", meta.getPartitionKey().get(0).getName());
		assertEquals(DataType.text(), meta.getPartitionKey().get(0).getType());
		assertEquals("key2", meta.getPartitionKey().get(1).getName());
		assertEquals(DataType.cint(), meta.getPartitionKey().get(1).getType());		
	}
	
	@Auditable
	@Table(name="complex_primary_key_entity")
	public class ComplexPrimaryKeyEntity {
		
		@PartitionKey(0)
		private String key;

		@PartitionKey(1)
		private Integer key2;
		
		@ClusteringColumn(0)
		private String subtype;

		@ClusteringColumn(1)
		private String subtype2;
		
		private String regularColumn;
		
		public ComplexPrimaryKeyEntity(String key, Integer key2) {
			this.key = key;
			this.key2 = key2;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Integer getKey2() {
			return key2;
		}

		public void setKey2(Integer key2) {
			this.key2 = key2;
		}

		public String getSubtype() {
			return subtype;
		}

		public void setSubtype(String subtype) {
			this.subtype = subtype;
		}

		public String getSubtype2() {
			return subtype2;
		}

		public void setSubtype2(String subtype2) {
			this.subtype2 = subtype2;
		}

		public String getRegularColumn() {
			return regularColumn;
		}

		public void setRegularColumn(String regularColumn) {
			this.regularColumn = regularColumn;
		}
	}	

	@Auditable
	@Table(name="custom_column_names_entity")
	public class CustomColumnNamesEntity {
		
		@PartitionKey(0)
		private String key;

		@PartitionKey(1)
		@Column(name="key_second")
		private Integer key2;
		
		public CustomColumnNamesEntity(String key, Integer key2) {
			this.key = key;
			this.key2 = key2;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Integer getKey2() {
			return key2;
		}

		public void setKey2(Integer key2) {
			this.key2 = key2;
		}		
	}	

	@Test
	public void test_custom_column_names_entity() {		
		manager.mapper(CustomColumnNamesEntity.class);
		assertTrue(tables().contains("audit_custom_column_names_entity"));
		TableMetadata meta = tableMetadata("audit_custom_column_names_entity");
		assertEquals(2, meta.getPartitionKey().size());
		assertEquals("key", meta.getPartitionKey().get(0).getName());
		assertEquals(DataType.text(), meta.getPartitionKey().get(0).getType());
		assertEquals("key_second", meta.getPartitionKey().get(1).getName());
		assertEquals(DataType.cint(), meta.getPartitionKey().get(1).getType());		
	}
	
	@Test
	public void test_complex_primary_key_entity() {		
		manager.mapper(ComplexPrimaryKeyEntity.class);
		assertTrue(tables().contains("audit_complex_primary_key_entity"));
		TableMetadata meta = tableMetadata("audit_complex_primary_key_entity");
		assertEquals(4, meta.getPartitionKey().size());
		assertEquals("key", meta.getPartitionKey().get(0).getName());
		assertEquals(DataType.text(), meta.getPartitionKey().get(0).getType());
		assertEquals("key2", meta.getPartitionKey().get(1).getName());
		assertEquals(DataType.cint(), meta.getPartitionKey().get(1).getType());		
		assertEquals("subtype", meta.getPartitionKey().get(2).getName());
		assertEquals(DataType.text(), meta.getPartitionKey().get(2).getType());		
		assertEquals("subtype2", meta.getPartitionKey().get(3).getName());
		assertEquals(DataType.text(), meta.getPartitionKey().get(3).getType());		
	}	
	
	@Auditable
	@Table(name="auditable_entity")
	public class AuditableEntity {
		
		@PartitionKey
		private String key;

		public AuditableEntity(String key) {
			super();
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}	

	@Test
	public void test_auditable_entity() {		
		Mapper<AuditableEntity> mapper = manager.mapper(AuditableEntity.class);
		AuditableEntity entity = new AuditableEntity("test-key");
		mapper.save(entity);
		assertTrue(tables().contains("audit_auditable_entity"));
		TableMetadata meta = tableMetadata("audit_auditable_entity");
		assertEquals(1, meta.getPartitionKey().size());
		assertEquals("key", meta.getPartitionKey().get(0).getName());
		assertEquals(1, meta.getClusteringColumns().size());
		assertEquals("time", meta.getClusteringColumns().get(0).getName());
		assertEquals(DataType.text(), meta.getColumn("cql").getType());
		assertEquals(DataType.text(), meta.getColumn("err").getType());
		assertEquals(DataType.text(), meta.getColumn("values").getType());
		assertEquals(DataType.text(), meta.getColumn("type").getType());
		assertEquals(DataType.bigint(), meta.getColumn("exec").getType());
		ResultSet result = session.execute("SELECT * FROM audit_auditable_entity");
		List<Row> rows = result.all();
		assertEquals(1, rows.size());
	}
	
	@Auditable
	@Table(name="column_exclusion_entity")
	public class ColumnExclusionEntity {
		
		@PartitionKey
		private String key;

		private String col1;
		
		@AuditExclusion
		private String col2;

		@AuditExclusion
		@Column(name = "column3")		
		private String col3;

		public ColumnExclusionEntity(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getCol1() {
			return col1;
		}

		public void setCol1(String col1) {
			this.col1 = col1;
		}

		public String getCol2() {
			return col2;
		}

		public void setCol2(String col2) {
			this.col2 = col2;
		}

		public String getCol3() {
			return col3;
		}

		public void setCol3(String col3) {
			this.col3 = col3;
		}
	}	

	@Test
	public void test_column_exclusion_entity() {		
		Mapper<ColumnExclusionEntity> mapper = manager.mapper(ColumnExclusionEntity.class);
		ColumnExclusionEntity entity = new ColumnExclusionEntity("test-key");
		entity.setCol1("not-a-big-secret");
		entity.setCol2("very-hush-hush");
		entity.setCol3("if-i-told-you...");
		mapper.save(entity);
		assertTrue(tables().contains("audit_column_exclusion_entity"));
		ResultSet result = session.execute("SELECT * FROM audit_column_exclusion_entity");
		List<Row> rows = result.all();
		assertEquals(1, rows.size());
		Row row = rows.get(0);
		String values = row.getString("values");
		System.out.println(values);
		assertNotSame(-1, values.indexOf("not-a-big-secret"));
		assertEquals(-1, values.indexOf("very-hush-hush"));
		assertEquals(-1, values.indexOf("if-i-told-you..."));
	}	
	
}
