package org.rapidprom.external.connectors.prom;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.PluginExecutionResult;
import org.processmining.framework.plugin.PluginManager;
import org.processmining.framework.plugin.ProMFuture;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.impl.AbstractGlobalContext;
import org.processmining.framework.plugin.impl.PluginExecutionResultImpl;
import org.processmining.framework.plugin.impl.PluginManagerImpl;

public final class RapidProMGlobalContext extends AbstractGlobalContext {

	private static boolean initialized = false;
	private static RapidProMGlobalContext instance = null;

	public static RapidProMGlobalContext initialize(PluginManager pluginManager) {
		instance = new RapidProMGlobalContext(pluginManager);
		initialized = true;
		return instance;
	}

	public static RapidProMGlobalContext instance() {
		assert (initialized);
		return instance;
	}

	private final PluginContext context = new RapidProMPluginContext(this, "RapidProM root plugin context");

	private final PluginManager pluginManager;

	private RapidProMGlobalContext(PluginManager pluginManager) {
		this.pluginManager = pluginManager;
	}

	private ProMFuture<?>[] createProMFutures(Plugin pluginAnn) {
		ProMFuture<?>[] futures = new ProMFuture<?>[pluginAnn.returnTypes().length];
		for (int i = 0; i < pluginAnn.returnTypes().length; i++) {
			futures[i] = new ProMFuture<Object>(pluginAnn.returnTypes()[i], pluginAnn.returnLabels()[i]) {
				@Override
				protected Object doInBackground() throws Exception {
					// NOP
					return null;
				}
			};
		}
		return futures;
	}

	@SuppressWarnings("unchecked")
	private <T extends Annotation> T findAnnotation(Annotation[] annotations, Class<T> clazz) {
		T result = null;
		for (Annotation a : annotations) {
			if (a.annotationType().equals(clazz)) {
				result = (T) a;
				break;
			}
		}
		return result;
	}

	/**
	 * This method prepares a PluginContext object, which is a child object of
	 * the PluginContext provided by the "PluginContextManager". Basically this
	 * method mimics some of the internal workings of the ProM framework, e.g.
	 * setting the future result objects.
	 * 
	 * @param classContainingProMPlugin
	 *            the class that contains the ProM plugin code
	 * @return
	 */
	public PluginContext getFutureResultAwarePluginContext(Class<?> classContainingProMPlugin) {
		assert (initialized);
		final PluginContext result = instance.getMainPluginContext()
				.createChildContext("RapidProMPluginContext_" + System.currentTimeMillis());
		Plugin pluginAnn = findAnnotation(classContainingProMPlugin.getAnnotations(), Plugin.class);

		PluginExecutionResult per = new PluginExecutionResultImpl(pluginAnn.returnTypes(), pluginAnn.returnLabels(),
				RapidProMGlobalContext.instance().getPluginManager()
						.getPlugin(classContainingProMPlugin.getCanonicalName()));
		ProMFuture<?>[] futures = createProMFutures(pluginAnn);
		Method m;
		try {
			m = PluginExecutionResultImpl.class.getDeclaredMethod("setResult", Object[].class);
			m.setAccessible(true);
			m.invoke(per, new Object[] { futures });
			result.setFuture(per);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	protected PluginContext getMainPluginContext() {
		return context;
	}

	public PluginContext getPluginContext() {
		return getMainPluginContext();
	}

	@Override
	public Class<? extends PluginContext> getPluginContextType() {
		return RapidProMPluginContext.class;
	}

	@Override
	public PluginManager getPluginManager() {
		return pluginManager;
	}

	public RapidProMPluginContext getRapidProMPluginContext() {
		return (RapidProMPluginContext) getMainPluginContext();
	}

}
