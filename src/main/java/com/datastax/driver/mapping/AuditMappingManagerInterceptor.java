package com.datastax.driver.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

/**
 * A ButeBuddy intercepter/delegation class for instrumenting
 * {@link MappingManager} in order to enable auditing.
 */
public class AuditMappingManagerInterceptor {

	/**
	 * Intercepter method for {@link MappingManager#getMapper} method that
	 * modifies behavior of the original factory method and returns
	 * {@link AuditManager} instance instead of {@link Mapper}.
	 * 
	 * @param klass
	 * @param manager
	 * @param managerClass
	 * @return
	 */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @RuntimeType
    public static <T> Mapper<T> getMapper(Class<T> klass, @This Object manager, @Origin Class managerClass) {
    	Map<Class<?>, Mapper<?>> mappers = getMappers(manager, managerClass);
        Mapper<T> mapper = (Mapper<T>)mappers.get(klass);
        if (mapper == null) {
            synchronized (mappers) {
                mapper = (Mapper<T>)mappers.get(klass);
                if (mapper == null) {
                    EntityMapper<T> entityMapper = getEntityMapper(klass, manager);
                    try {
						mapper = (Mapper<T>)instrumentMapper(manager, klass, entityMapper);
					} catch (Exception e) {
						throw new IllegalArgumentException(e);
					}
                    Map<Class<?>, Mapper<?>> newMappers = new HashMap<Class<?>, Mapper<?>>(mappers);
                    newMappers.put(klass, mapper);
                    mappers = newMappers;
                }
            }
        }
        return mapper;
    }
    
    /**
     * A helper method for invoking {@link AnnotationParser#parseEntity(Class, com.datastax.driver.mapping.EntityMapper.Factory, MappingManager)}
     * method using reflection.
     * 
     * @param klass the entity's class object
     * @param manager a reference to the {@link MappingManager} instance
     * @return an instance of {@link EntityMapper} for the given entity class
     */
    @SuppressWarnings("unchecked")
	private static <T> EntityMapper<T> getEntityMapper(Class<T> klass, Object manager) {
    	Method[] methods = AnnotationParser.class.getDeclaredMethods();
    	Method parseEntityMethod = null;
    	
    	for (Method m : methods) {
    		if ("parseEntity".equals(m.getName())) {
    			parseEntityMethod = m;
    		}
    	}
    	if (parseEntityMethod != null) {
    		try {
				return (EntityMapper<T>)parseEntityMethod.invoke(manager, klass, ReflectionMapper.factory(), manager);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new IllegalArgumentException(e);
			}
    	} else {
    		throw new IllegalStateException("Unable to find AnnotationParser.parseEntity method.");
    	}
    }
    
    /**
     * A helper method for obtaining a reference to the private field {@link MappingManager#mappers} using
     * reflection.
     * 
     * @param manager a reference to the {@link MappingManager} instance
     * @param managerClass a reference to the {@link MappingManager} class object 
     * @return value of the given manager's private field {@code mappers}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static Map<Class<?>, Mapper<?>> getMappers(Object manager, Class managerClass) {
        Field f = null;
        Map<Class<?>, Mapper<?>> val = null;
        try {
            f = managerClass.getDeclaredField("mappers");
            f.setAccessible(true);
            val = (Map<Class<?>, Mapper<?>>)f.get(manager);
        } catch (NoSuchFieldException nsfe) {
        	throw new IllegalStateException(nsfe);
        } catch (IllegalAccessException e) {
        	throw new IllegalStateException(e);
		}
        return val;
    }
    
    /**
     * Instantiate {@link AuditMapper} using reflection in order to avoid
     * mentioning {@link MappingManager} class. This is necessary because
     * this action is executed in the process of {@link MappingManager} 
     * instrumentation.  
     *  
     * @param manager a reference to the originating {@link MappingManager} instance 
     * @param klass the entity class 
     * @param mapper a respective {@link EntityManager}
     * @return a {@link AuditManager} instance
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
	private static <T> Mapper<T> instrumentMapper(Object manager, Class<T> klass, EntityMapper<T> mapper) {   	
    	@SuppressWarnings("rawtypes")
		Constructor[] ctors = AuditMapper.class.getDeclaredConstructors();
    	
    	@SuppressWarnings("rawtypes")
		Constructor ctor = ctors[0];
    	ctor.setAccessible(true);
    	
    	try {
			return (Mapper<T>)ctor.newInstance(manager, klass, mapper);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new IllegalStateException("Unable to instantiate AuditMapper.", e);
		}
    }    

}
