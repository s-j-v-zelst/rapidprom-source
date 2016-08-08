package org.rapidprom.external.connectors.prom;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.processmining.framework.plugin.GlobalContext;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.impl.AbstractPluginContext;

public class RapidProMPluginContext extends AbstractPluginContext {

	private final Executor executor;

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

	@Override
	public Executor getExecutor() {
		return executor;
	}

	@Override
	protected PluginContext createTypedChildContext(String label) {
		return new RapidProMPluginContext(this, label);
	}

}
