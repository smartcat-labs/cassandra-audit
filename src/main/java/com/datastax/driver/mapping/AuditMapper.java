package com.datastax.driver.mapping;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.smartcat.cassandra_audit.Auditable;

public class AuditMapper<T> extends Mapper<T> {

	private static final EnumMap<Option.Type, Option> NO_OPTIONS = new EnumMap<Option.Type, Option>(Option.Type.class);
	private static int EXECUTOR_NO_THREADS = 10;
	private static ExecutorService executor = Executors.newFixedThreadPool(EXECUTOR_NO_THREADS);
	
	private class AuditOptions {
		boolean auditable;
		boolean before;
		boolean after;
		String tableName;
		
		AuditOptions(Class<T> klass) {
			Auditable annotation = klass.getAnnotation(Auditable.class);
			if (annotation != null) {
				this.auditable = true;
				this.tableName = annotation.tableName();
				if (this.tableName.isEmpty()) {
					this.tableName = annotation.tablePrefix() + klass.getSimpleName().toLowerCase();
				}
				this.before = annotation.before();
				this.after = annotation.after();
			} else {
				this.auditable = false;
			} 			
		}
	}
	
	private AuditOptions auditOptions;
    private volatile EnumMap<Option.Type, Option> defaultSaveOptions;
    private volatile EnumMap<Option.Type, Option> defaultDeleteOptions;
    
	public AuditMapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
		super(manager, klass, mapper);
        this.defaultSaveOptions = NO_OPTIONS;
        this.defaultDeleteOptions = NO_OPTIONS;
        this.auditOptions = new AuditOptions(klass);
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
		auditSaveBefore(entity);
		super.save(entity);
		auditSaveAfter(entity);
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#save(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public void save(T entity, Option... options) {
		auditSaveBefore(entity, options);
		super.save(entity, options);
		auditSaveAfter(entity, options);
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#saveAsync(java.lang.Object)
	 */
	@Override
	public ListenableFuture<Void> saveAsync(final T entity) {
		auditSaveBefore(entity);
		ListenableFuture<Void> res = super.saveAsync(entity);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable arg0) {
				// Do nothing.
			}

			@Override
			public void onSuccess(Void arg0) {
				auditSaveAfter(entity);
			}
		});		

		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#saveAsync(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public ListenableFuture<Void> saveAsync(final T entity, final Option... options) {
		auditSaveBefore(entity, options);
		ListenableFuture<Void> res = super.saveAsync(entity, options);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable arg0) {
				// Do nothing.
			}

			@Override
			public void onSuccess(Void arg0) {
				auditSaveAfter(entity, options);
			}
		});			
		
		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#delete(java.lang.Object)
	 */
	@Override
	public void delete(T entity) {
		auditDeleteBefore(entity);
		super.delete(entity);
		auditDeleteAfter(entity);
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#delete(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public void delete(T entity, Option... options) {
		auditDeleteBefore(entity, options);
		super.delete(entity, options);
		auditDeleteAfter(entity, options);
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#deleteAsync(java.lang.Object)
	 */
	@Override
	public ListenableFuture<Void> deleteAsync(final T entity) {
		auditDeleteBefore(entity);
		ListenableFuture<Void> res = super.deleteAsync(entity);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable arg0) {
				// Do nothing.
			}

			@Override
			public void onSuccess(Void arg0) {
				auditDeleteAfter(entity);
			}
		});		

		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#deleteAsync(java.lang.Object, com.datastax.driver.mapping.Mapper.Option[])
	 */
	@Override
	public ListenableFuture<Void> deleteAsync(final T entity, final Option... options) {
		auditDeleteBefore(entity, options);
		ListenableFuture<Void> res = super.deleteAsync(entity, options);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable arg0) {
				// Do nothing.
			}

			@Override
			public void onSuccess(Void arg0) {
				auditDeleteAfter(entity, options);
			}
		});		
		
		return res;
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#delete(java.lang.Object[])
	 */
	@Override
	public void delete(Object... objects) {
		auditDeleteBefore(objects);
		super.delete(objects);
		auditDeleteAfter(objects);
	}


	/* (non-Javadoc)
	 * @see com.datastax.driver.mapping.Mapper#deleteAsync(java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Void> deleteAsync(final Object... objects) {
		auditDeleteBefore(objects);
		ListenableFuture<Void> res = super.deleteAsync(objects);
    	Futures.addCallback(res, new FutureCallback<Void>() {
			@Override
			public void onFailure(Throwable arg0) {
				// Do nothing.
			}

			@Override
			public void onSuccess(Void arg0) {
				auditDeleteAfter(objects);
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
    
    private void auditSaveBefore(T entity) {
    	auditSave(true, entity, (Mapper.Option[])null);
    }
    
    private void auditSaveBefore(T entity, Option... options) {
    	auditSave(true, entity, options);
    }

    private void auditSaveAfter(T entity) {
    	auditSave(false, entity, (Mapper.Option[])null);
    }
    
    private void auditSaveAfter(T entity, Option... options) {
    	auditSave(false, entity, options);
    }
    
    private void auditDeleteBefore(T entity) {
    	auditDelete(true, entity, (Object[])null);
    }
    
    private void auditDeleteBefore(T entity, Option... options) {
    	auditDelete(true, entity, (Object[])options);
    }
    
    private void auditDeleteBefore(Object... objects) {
    	auditDelete(true, null, objects);
    }
    
    private void auditDeleteAfter(T entity) {
    	auditDelete(false, entity, (Object[])null);
    }
    
    private void auditDeleteAfter(T entity, Option... options) {
    	auditDelete(false, entity, (Object[])options);
    }
    
    private void auditDeleteAfter(Object... objects) {
    	auditDelete(false, null, objects);
    }
    
    private void auditSave(final boolean before, final T entity, final Option... options) {
    	if (auditOptions.auditable && 
    			((before && auditOptions.before) || (!before && auditOptions.after))) {
    		// execute the rest of audit action asynchronously  
			executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					auditSaveExec(before, entity, options);
					return null;
				}
			});    		
    	}
    }
    
    private void auditDelete(final boolean before, final T entity, final Object... objects) {
    	if (auditOptions.auditable && 
    			((before && auditOptions.before) || (!before && auditOptions.after))) {
    		// execute the rest of audit action asynchronously
			executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					auditDeleteExec(before, entity, objects);
					return null;
				}
			});    		
    	}    	
    }
     
    private void auditSaveExec(final boolean before, final T entity, final Option... options) {
    	BoundStatement bs;
    	if (options == null) {
    		bs = (BoundStatement)saveQuery(entity);
    	} else {
    		bs = (BoundStatement)saveQuery(entity, options);
    	}
    	PreparedStatement ps = bs.preparedStatement();
    	
    	String statementQuery = bs.preparedStatement().getQueryString();
    	List<String> statementValues = new ArrayList<String>();
    	for (ColumnDefinitions.Definition def : ps.getVariables()) {
    		statementValues.add(def.getName() + "[" + def.getType() + "]:" + bs.getObject(def.getName()));
    	}
    	
    	List<String> auditPrimaryKey = new ArrayList<String>();
    	for (ColumnMetadata cm : getTableMetadata().getPrimaryKey()) {
    		auditPrimaryKey.add(cm.getName() + ":" + bs.getObject(cm.getName()));
    	}
    	
    	System.out.println("Save mutation [" + (before ? "before" : "after") + "]: " + entity.getClass().getSimpleName() + ": " +
    			statementQuery + ": " + statementValues + ": primary key: " + auditPrimaryKey);
    } 
    
    private void auditDeleteExec(final boolean before, final T entity, final Object... objects) {
    	BoundStatement bs;
    	if (objects == null) {
    		bs = (BoundStatement)deleteQuery(entity);
    	} else {
    		bs = (BoundStatement)deleteQuery(entity, objects);
    	}
    	PreparedStatement ps = bs.preparedStatement();
    	
    	String statementQuery = bs.preparedStatement().getQueryString();
    	List<String> statementValues = new ArrayList<String>();
    	for (ColumnDefinitions.Definition def : ps.getVariables()) {
    		statementValues.add(def.getName() + "[" + def.getType() + "]:" + bs.getObject(def.getName()));
    	}
    	
    	List<String> auditPrimaryKey = new ArrayList<String>();
    	for (ColumnMetadata cm : getTableMetadata().getPrimaryKey()) {
    		auditPrimaryKey.add(cm.getName() + ":" + bs.getObject(cm.getName()));
    	}
    	
    	System.out.println("Delete mutation [" + (before ? "before" : "after") + "]: " + entity.getClass().getSimpleName() + ": " +
    			statementQuery + ": " + statementValues + ": primary key: " + auditPrimaryKey);    }    
}
