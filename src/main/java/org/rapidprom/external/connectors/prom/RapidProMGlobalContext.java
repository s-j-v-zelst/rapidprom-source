package org.rapidprom.external.connectors.prom;

import java.lang.annotation.Annotation;

import org.processmining.framework.connections.ConnectionManager;
import org.processmining.framework.connections.impl.ConnectionManagerImpl;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.PluginManager;
import org.processmining.framework.plugin.ProMFuture;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.impl.AbstractGlobalContext;

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

	private final ConnectionManager connMgr;

	private final PluginContext context = new RapidProMPluginContext(this, "RapidProM root plugin context");

	private final PluginManager pluginManager;

	private RapidProMGlobalContext(PluginManager pluginManager) {
		this.pluginManager = pluginManager;
		this.connMgr = new ConnectionManagerImpl(pluginManager);
	}

	@Override
	public ConnectionManager getConnectionManager() {
		return connMgr;
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
				.createChildContext("rprom_child_context_" + System.currentTimeMillis());
		Plugin pluginAnn = findAnnotation(classContainingProMPlugin.getAnnotations(), Plugin.class);
		RapidProMPluginExecutionResultImpl per = new RapidProMPluginExecutionResultImpl(pluginAnn.returnTypes(),
				pluginAnn.returnLabels(), RapidProMGlobalContext.instance().getPluginManager()
						.getPlugin(classContainingProMPlugin.getCanonicalName()));
		ProMFuture<?>[] futures = createProMFutures(pluginAnn);
		per.setRapidProMFuture(futures);
		result.setFuture(per);
		return result;
	}

	@Override
	protected PluginContext getMainPluginContext() {
		return context;
	}

	public PluginContext getPluginContext() {
		return getMainPluginContext().createChildContext("rprom_child_context_" + System.currentTimeMillis());
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
