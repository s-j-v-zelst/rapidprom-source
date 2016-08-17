package org.rapidprom.external.connectors.prom;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.processmining.framework.plugin.GlobalContext;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.impl.AbstractPluginContext;

public class RapidProMPluginContext extends AbstractPluginContext {

	private final ExecutorService executor;

	public RapidProMPluginContext(GlobalContext context, String label) {
		super(context, label);
		executor = Executors.newCachedThreadPool();
	}

	public RapidProMPluginContext(RapidProMPluginContext context, String label) {
		super(context, label);
		if (context == null) {
			executor = Executors.newCachedThreadPool();
		} else {
			executor = context.getExecutor();
		}
	}

	public void closeExecutor(){
		executor.shutdownNow();
	}
	
	@Override
	public ExecutorService getExecutor() {
		return executor;
	}

	@Override
	protected PluginContext createTypedChildContext(String label) {
		return new RapidProMPluginContext(this, label);
	}

	@Override
	public void clear() {
		for (PluginContext c : getChildContexts()) {
			c.clear();
		}
		super.clear();
	}

}
