package io.smartcat.cassandra_audit;

import static net.bytebuddy.matcher.ElementMatchers.named;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.AuditMappingManagerInterceptor;
import com.datastax.driver.mapping.MappingManager;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

/**
 * <code>AuditManager</code> is a factory for audit-augmented
 * {@link MappingManager} instances. 
 */
public class AuditManager {
	
	/**
	 * Returns an modified instance of {@link MappingManger} that,
	 * in turn, generates {@link AuditMapper} instead of plain {@link Mapper}.
	 * 
	 * @param session a connection to a Cassandra cluster
	 * @return instrumented instance of {@link MappingManager}
	 */
	public static MappingManager getMappingManager(Session session) {
		Class<?> mappingManagerClass = new ByteBuddy()
			.subclass(MappingManager.class)
			.method(named("mapper"))
			.intercept(MethodDelegation.to(AuditMappingManagerInterceptor.class))
			.make()
			.load(AuditManager.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
			.getLoaded();
	    	
			@SuppressWarnings("rawtypes")
			Constructor ctor;
			try {
				ctor = mappingManagerClass.getDeclaredConstructor(Session.class);
				return (MappingManager)ctor.newInstance(session);
			} catch (NoSuchMethodException | SecurityException | InstantiationException 
					| IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new IllegalStateException("Unable to instantiate instrumented MappingManager.", e);
			}
	
	}

}
