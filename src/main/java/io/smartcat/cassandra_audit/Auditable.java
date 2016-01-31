/**
 * 
 */
package io.smartcat.cassandra_audit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.datastax.driver.mapping.annotations.Table;

/**
 * Indicates that an entity class (annotated with {@link Table})
 * should be audited. 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Auditable {

	/**
	 * Default audit table name prefix. It is used only
	 * if {@link Auditable#tableName()} is not set. The full
	 * table name is constructed concatenating the table prefix
	 * and the entity's simple class name.
	 */
	String tablePrefix() default "audit_";
	
	/**
	 * If set, specifies the full table name.
	 */
	String tableName() default "";
	
	/**
	 * If set, specifies the used keyspace. 
	 */
	String keyspaceName() default "";
	
	/**
	 * Controls whether an audit record should be issued
	 * before the actual entity mutation. The default
	 * value is {@code true}.
	 */
	boolean before() default true;
	
	/**
	 * Controls whether an audit record should be issued
	 * after the actual entity mutation. The default
	 * value is {@code false}.
	 */	
	boolean after() default false;
}
