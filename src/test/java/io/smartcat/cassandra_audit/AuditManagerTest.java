package io.smartcat.cassandra_audit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;

import junit.framework.TestCase;

public class AuditManagerTest extends TestCase {
	
	@Test
	public void testMapperManager() {
		Session session = mock(Session.class);
		Cluster cluster = mock(Cluster.class);
		Metadata metadata = mock(Metadata.class);
		Configuration configuration = mock(Configuration.class);
		ProtocolOptions pOptions = mock(ProtocolOptions.class);
		
		when(session.getCluster()).thenReturn(cluster);
		when(cluster.getConfiguration()).thenReturn(configuration);
		when(cluster.getMetadata()).thenReturn(metadata);
		when(configuration.getProtocolOptions()).thenReturn(pOptions);
		when(pOptions.getProtocolVersionEnum()).thenReturn(ProtocolVersion.V3);
		
		Object mappingManager = AuditManager.getMappingManager(session);
		assertTrue(mappingManager instanceof MappingManager);
		assertNotSame(MappingManager.class.getName(), mappingManager.getClass().getName());
	}
}
