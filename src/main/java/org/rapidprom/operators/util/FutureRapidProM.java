package org.rapidprom.operators.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.processmining.framework.plugin.PluginContext;

/**
 * This future class overrides the cancel method, invoking the plugin context'
 * progress bar's cancel method before actually cancelling the future(thus,
 * deleting the child contexts)
 * 
 * @author abolt
 *
 * @param <T>
 */
public class FutureRapidProM<T> implements Future<T> {

	private final PluginContext pluginContext;
	private final Future<T> oldFuture;

	public FutureRapidProM(Future<T> oldFuture, PluginContext pluginContext) {
		this.oldFuture = oldFuture;
		this.pluginContext = pluginContext;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		pluginContext.getProgress().cancel();
		return oldFuture.cancel(mayInterruptIfRunning);
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return oldFuture.get();
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return oldFuture.get(timeout, unit);
	}

	@Override
	public boolean isCancelled() {

		return oldFuture.isCancelled();
	}

	@Override
	public boolean isDone() {

		return oldFuture.isDone();
	}

}
