package io.smartcat.cassandra_audit;

import static net.bytebuddy.matcher.ElementMatchers.declaresMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;

import java.lang.instrument.Instrumentation;

import com.datastax.driver.mapping.AuditMappingManagerInterceptor;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;

/**
 * {@code AuditAgent} acts as a Java agent used to instrument 
 * original DataStax driver's classes in order to enable auditing. 
 */
public class AuditAgent {
	
	public static void premain(String arg, Instrumentation inst) {
		// Transformer for MappingManager
		new AgentBuilder.Default()
		//TODO: identify MappingManager by its TypeDescription
		.type(declaresMethod(named("getMapper")).and(declaresMethod(named("getUDTMapper"))))
		.transform(new AgentBuilder.Transformer() {
			@Override
			public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder,
					TypeDescription typeDescription) {
				return builder
						.method(named("getMapper"))
						.intercept(MethodDelegation.to(AuditMappingManagerInterceptor.class));				
			}
		})
		.installOn(inst);
	}
}
