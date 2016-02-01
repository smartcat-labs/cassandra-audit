package com.datastax.driver.mapping;

import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.datastax.driver.core.BoundStatement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.smartcat.cassandra_audit.Auditable;

public class AuditMapper<T> extends Mapper<T> {

	private static final EnumMap<Option.Type, Option> NO_OPTIONS = new EnumMap<Option.Type, Option>(Option.Type.class);
	private static int EXECUTOR_NO_THREADS = 10;
	private static ExecutorService executor = Executors.newFixedThreadPool(EXECUTOR_NO_THREADS);

	class AuditOptions {
		boolean auditable;
		String tableName;
		String keyspaceName;
		
		AuditOptions(Class<T> klass) {
			Auditable annotation = klass.getAnnotation(Auditable.class);
			if (annotation != null) {
				this.auditable = true;
				this.tableName = annotation.tableName();
				if (this.tableName.isEmpty()) {
					this.tableName = annotation.tablePrefix() + klass.getSimpleName().toLowerCase();
				}
				this.keyspaceName = annotation.keyspaceName();
				if (this.keyspaceName.isEmpty()) {
					this.keyspaceName = mapper.getKeyspace();
				}
			} else {
				this.auditable = false;
			} 			
		}
	}
	
	AuditOptions auditOptions;
    private volatile EnumMap<Option.Type, Option> defaultSaveOptions;
    private volatile EnumMap<Option.Type, Option> defaultDeleteOptions;
    private AuditLogger auditLogger;
    
	public AuditMapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
		super(manager, klass, mapper);
        this.defaultSaveOptions = NO_OPTIONS;
        this.defaultDeleteOptions = NO_OPTIONS;
        this.auditOptions = new AuditOptions(klass);
        this.auditLogger = CassandraAuditLogger.getInstance(manager.getSession());
        this.auditLogger.init(this);
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#resetDefaultSaveOptions()
	 */
	@Override
	public void resetDefaultSaveOptions() {
		super.resetDefaultSaveOptions();
		this.defaultSaveOptions = NO_OPTIONS;
	}

	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#resetDefaultDeleteOptions()
	 */
	@Override
	public void resetDefaultDeleteOptions() {
		super.resetDefaultDeleteOptions();
		this.defaultDeleteOptions = NO_OPTIONS;
	}
		
	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#setDefaultSaveOptions(com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public void setDefaultSaveOptions(Option... options) {
		super.setDefaultSaveOptions(options);
		this.defaultSaveOptions = toMap(options);
	}

	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#setDefaultDeleteOptions(com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public void setDefaultDeleteOptions(Option... options) {
		super.setDefaultDeleteOptions(options);
		this.defaultDeleteOptions = toMap(options);
	}
	
	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#save(java.lang.Object)
	 */
	@Override
	public void save(T entity) {
		final long start = System.nanoTime();
		try {
			super.save(entity);
			final long execTime = System.nanoTime() - start;
			auditSave(execTime, null, entity);			
		} catch (Exception err) {
			final long execTime = System.nanoTime() - start;
			auditSave(execTime, err.getMessage(), entity);
			throw err;
		}
	}

	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#save(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public void save(T entity, Option... options) {
		final long start = System.nanoTime();
		try {
			super.save(entity, options);
			final long execTime = System.nanoTime() - start;
			auditSave(execTime, null, entity, options);			
		} catch (Exception err) {
			final long execTime = System.nanoTime() - start;
			auditSave(execTime, err.getMessage(), entity, options);
			throw err;
		}		
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#saveAsync(java.lang.Object)
	 */
	@Override
	public ListenableFuture<Void> saveAsync(final T entity) {
		final long start = System.nanoTime();
		ListenableFuture<Void> res = super.saveAsync(entity);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable err) {
				final long execTime = System.nanoTime() - start;
				auditSave(execTime, err.getMessage(), entity);				
			}

			@Override
			public void onSuccess(Void arg0) {
				final long execTime = System.nanoTime() - start;
				auditSave(execTime, null, entity);
			}
		});		

		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#saveAsync(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public ListenableFuture<Void> saveAsync(final T entity, final Option... options) {
		final long start = System.nanoTime();
		ListenableFuture<Void> res = super.saveAsync(entity, options);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable err) {
				final long execTime = System.nanoTime() - start;
				auditSave(execTime, err.getMessage(), entity, options);
			}

			@Override
			public void onSuccess(Void arg0) {
				final long execTime = System.nanoTime() - start;
				auditSave(execTime, null, entity, options);
			}
		});			
		
		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#delete(java.lang.Object)
	 */
	@Override
	public void delete(T entity) {
		final long start = System.nanoTime();
		try {
			super.delete(entity);
			final long execTime = System.nanoTime() - start;
			auditDelete(execTime, null, entity);			
		} catch (Exception err) {
			final long execTime = System.nanoTime() - start;
			auditDelete(execTime, err.getMessage(), entity);
			throw err;
		}		
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#delete(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public void delete(T entity, Option... options) {
		final long start = System.nanoTime();
		try {
			super.delete(entity, options);
			final long execTime = System.nanoTime() - start;
			auditDelete(execTime, null, entity, options);			
		} catch (Exception err) {
			final long execTime = System.nanoTime() - start;
			auditDelete(execTime, err.getMessage(), entity, options);
			throw err;
		}		
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#deleteAsync(java.lang.Object)
	 */
	@Override
	public ListenableFuture<Void> deleteAsync(final T entity) {
		final long start = System.nanoTime();
		ListenableFuture<Void> res = super.deleteAsync(entity);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable err) {
				final long execTime = System.nanoTime() - start;
				auditDelete(execTime, err.getMessage(), entity);
			}

			@Override
			public void onSuccess(Void arg0) {
				final long execTime = System.nanoTime() - start;
				auditDelete(execTime, null, entity);
			}
		});		

		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#deleteAsync(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public ListenableFuture<Void> deleteAsync(final T entity, final Option... options) {
		final long start = System.nanoTime();
		ListenableFuture<Void> res = super.deleteAsync(entity, options);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable err) {
				final long execTime = System.nanoTime() - start;
				auditDelete(execTime, err.getMessage(), entity, options);
			}

			@Override
			public void onSuccess(Void arg0) {
				final long execTime = System.nanoTime() - start;
				auditDelete(execTime, null, entity, options);
			}
		});		
		
		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#delete(java.lang.Object[])
	 */
	@Override
	public void delete(Object... objects) {
		final long start = System.nanoTime();
		try {
			super.delete(objects);
			final long execTime = System.nanoTime() - start;
			auditDelete(execTime, null, objects);			
		} catch (Exception err) {
			final long execTime = System.nanoTime() - start;
			auditDelete(execTime, err.getMessage(), objects);
			throw err;
		}		
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#deleteAsync(java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Void> deleteAsync(final Object... objects) {
		final long start = System.nanoTime();
		ListenableFuture<Void> res = super.deleteAsync(objects);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable err) {
				final long execTime = System.nanoTime() - start;
				auditDelete(execTime, err.getMessage(), objects);
			}

			@Override
			public void onSuccess(Void arg0) {
				final long execTime = System.nanoTime() - start;
				auditDelete(execTime, null, objects);
			}
		});		
		return res;
	}

    private static EnumMap<Option.Type, Option> toMap(Option[] options) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(Option.Type.class);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }
    
    private void auditSave(final long execTime, final String error, T entity) {
    	auditSaveAsync(execTime, error, entity, (Mapper.Option[])null);
    }
    
    private void auditSave(final long execTime, final String error, T entity, Option... options) {
    	auditSaveAsync(execTime, error, entity, options);
    }
    
    private void auditDelete(final long execTime, final String error, T entity) {
    	auditDeleteAsync(execTime, error, entity, (Object[])null);
    }
    
    private void auditDelete(final long execTime,  final String error, T entity, Option... options) {
    	auditDeleteAsync(execTime, error, entity, (Object[])options);
    }
    
    private void auditDelete(final long execTime, final String error, Object... objects) {
    	auditDeleteAsync(execTime, error, null, objects);
    }
    
    private void auditSaveAsync(final long execTime, final String error, final T entity, final Option... options) {
    	if (auditOptions.auditable) {
    		// execute the rest of audit action asynchronously  
			Future<Void> task = executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					auditSaveExec(execTime, error, entity, options);
					return null;
				}
			});
			try {
				task.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException("An error occured while executing audit.", e);
			}
    	}
    }
    
    private void auditDeleteAsync(final long execTime, final String error, final T entity, final Object... objects) {
    	if (auditOptions.auditable) {
    		// execute the rest of audit action asynchronously
			Future<Void> task = executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					auditDeleteExec(execTime, error, entity, objects);
					return null;
				}
			});
			try {
				task.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException("An error occured while executing audit.", e);
			}			
    	}    	
    }
     
    private void auditSaveExec(final long execTime, final String error, final T entity, final Option... options) {
    	BoundStatement bs;
    	if (options == null) {
    		bs = (BoundStatement)saveQuery(entity);
    	} else {
    		bs = (BoundStatement)saveQuery(entity, options);
    	}
    	auditLogger.log(execTime, error, bs);
    } 
    
    private void auditDeleteExec(final long execTime, final String error, final T entity, final Object... objects) {
    	BoundStatement bs;
    	if (objects == null) {
    		bs = (BoundStatement)deleteQuery(entity);
    	} else {
    		bs = (BoundStatement)deleteQuery(entity, objects);
    	}    
    	auditLogger.log(execTime, error, bs);
    }
}
