package org.rapidprom.operators.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.processmining.framework.plugin.PluginContext;


/**
 * This class is used to call ProM methods with a timeout.
 * 
 * This class extends an executor service, but does the submit() method
 * differently. The Futures provided by such method ate overriden in order to
 * give the plugin context a cancel signal before the childs are deleted
 * 
 * @author abolt
 *
 */

public class ExecutorServiceRapidProM implements ExecutorService {

	private final PluginContext pluginContext;
	private ExecutorService executor;

	public ExecutorServiceRapidProM(PluginContext pluginContext) {
		this.pluginContext = pluginContext;
		executor = Executors.newSingleThreadExecutor();
	}

	@Override
	public void execute(Runnable arg0) {
		executor.execute(arg0);
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return executor.awaitTermination(timeout, unit);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return executor.invokeAll(tasks);

	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {

		return executor.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {

		return executor.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return executor.invokeAny(tasks, timeout, unit);
	}

	@Override
	public boolean isShutdown() {
		return executor.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return executor.isTerminated();
	}

	@Override
	public void shutdown() {
		executor.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return executor.shutdownNow();
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {

		Future<T> result = executor.submit(task);
		FutureRapidProM<T> adjusted = new FutureRapidProM<T>(result, pluginContext);
		return adjusted;
	}

	@Override
	public Future<?> submit(Runnable task) {

		return executor.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return executor.submit(task, result);
	}

}
