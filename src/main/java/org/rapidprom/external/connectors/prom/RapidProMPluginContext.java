package org.rapidprom.external.connectors.prom;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.processmining.framework.plugin.GlobalContext;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.Progress;
import org.processmining.framework.plugin.impl.AbstractPluginContext;

public class RapidProMPluginContext extends AbstractPluginContext {

	private final Executor executor;
	private Progress progress;

	public RapidProMPluginContext(GlobalContext context, String label) {
		super(context, label);
		executor = Executors.newCachedThreadPool();
	}
	
	public RapidProMPluginContext(GlobalContext context, String label, Progress progress) {
		super(context, label);
		this.progress = progress;
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
	
	public PluginContext createChildContext(String label, Progress progress) {
		return new RapidProMPluginContext(this, label, progress);
	}

	@Override
	public void clear() {
		for (PluginContext c : getChildContexts())
			c.clear();
		super.clear();
	}

	@Override
	public Progress getProgress() {
		if (progress != null) {
			return progress;
		} else {
			return super.getProgress();	
		}		
	}

}
