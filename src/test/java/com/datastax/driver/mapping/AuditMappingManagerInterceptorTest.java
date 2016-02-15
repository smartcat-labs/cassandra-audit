package com.datastax.driver.mapping;

import static org.mockito.Mockito.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import junit.framework.TestCase;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CassandraAuditLogger.class)
public class AuditMappingManagerInterceptorTest  extends TestCase {

	private static Session session;
	
	@BeforeClass
	public static void before() {
		session = mock(Session.class);
		Cluster cluster = mock(Cluster.class);
		Metadata metadata = mock(Metadata.class);
		Configuration configuration = mock(Configuration.class);
		ProtocolOptions pOptions = mock(ProtocolOptions.class);
		
		when(session.getCluster()).thenReturn(cluster);
		when(cluster.getConfiguration()).thenReturn(configuration);
		when(cluster.getMetadata()).thenReturn(metadata);
		when(configuration.getProtocolOptions()).thenReturn(pOptions);
		when(pOptions.getProtocolVersionEnum()).thenReturn(ProtocolVersion.V3);
		
		PowerMockito.mockStatic(CassandraAuditLogger.class);
		CassandraAuditLogger auditLogger = mock(CassandraAuditLogger.class);
		when(CassandraAuditLogger.getInstance(session)).thenReturn(auditLogger);
	}
	
	@Table(name="entitya", keyspace="test")
	public class EntityA {
		
		@PartitionKey
		private String key;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}
	
	@Test
	public void testGetMapper() {
		MappingManager manager = new MappingManager(session);
		Mapper<EntityA> mapper = AuditMappingManagerInterceptor.getMapper(EntityA.class, manager, manager.getClass());
		assertTrue(mapper instanceof AuditMapper<?>);
		assertEquals(manager, mapper.getManager());
		PowerMockito.verifyStatic(Mockito.times(1));
		CassandraAuditLogger.getInstance(Mockito.eq(session));
	}

}
