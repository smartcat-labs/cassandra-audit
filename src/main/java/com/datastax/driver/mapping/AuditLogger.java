package com.datastax.driver.mapping;

import com.datastax.driver.core.BoundStatement;

/**
 * Implementations of this interface provide a way to store
 * Cassandra audit events (entity mutations).
 */
public interface AuditLogger {
	public <T> void init(AuditMapper<T> mapper);
	public void log(BoundStatement statement);
}
