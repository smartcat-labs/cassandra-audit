package io.smartcat.cassandra_audit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that allows for excluding a certain column(cell) value from
 * the audit log.
 * This annotation could be used to prevent security sensitive information or
 * large blob cell values to be stored in the audit log.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AuditExclusion {}
