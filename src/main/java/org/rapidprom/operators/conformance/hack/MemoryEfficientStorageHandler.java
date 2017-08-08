package org.rapidprom.operators.conformance.hack;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import nl.tue.astar.AStarException;
import nl.tue.astar.FastLowerBoundTail;
import nl.tue.astar.Head;
import nl.tue.astar.Record;
import nl.tue.astar.Tail;
import nl.tue.astar.impl.State;
import nl.tue.astar.impl.memefficient.CachedStorageAwareDelegate;
import nl.tue.astar.impl.memefficient.StorageAwareDelegate;
import nl.tue.storage.CompressedHashSet;
import nl.tue.storage.CompressedStore;
import nl.tue.storage.FastByteArrayInputStream;
import nl.tue.storage.StorageException;
import nl.tue.storage.impl.CompressedStoreHashSetImpl.Result;
import nl.tue.storage.impl.SkippableOutputStream;

public class MemoryEfficientStorageHandler<H extends Head, T extends Tail>
		implements AbstractAStarThread.StorageHandler<H, T> {

	protected final MemoryEfficientAStarAlgorithm<H, T> algorithm;
	protected final CompressedStore<State<H, T>> store;
	protected final CompressedHashSet<State<H, T>> statespace;
	protected final StorageAwareDelegate<H, T> delegate;

	protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public MemoryEfficientStorageHandler(
			MemoryEfficientAStarAlgorithm<H, T> algorithm) {
		// super(algorithm.getDelegate(), trace, maxStates);
		this.algorithm = algorithm;
		this.statespace = algorithm.getStatespace();
		this.store = algorithm.getStore();
		this.delegate = algorithm.getDelegate();
		// get the index where initialHead is stored
		// initializeQueue(initialHead);

	}

	public void storeStateForRecord(State<H, T> state, Record newRec)
			throws AStarException {
		final Result<State<H, T>> r;
		// synchronized (statespace) {
		try {
			lock.writeLock().lock();
			r = statespace.add(state);
		} catch (StorageException e) {
			throw new AStarException(e);
		} finally {
			lock.writeLock().unlock();
		}

		// }
		newRec.setState(r.index);
	}

	public long getIndexOf(H head) throws AStarException {
		// synchronized (statespace) {
		try {
			lock.readLock().lock();
			return statespace.contains(new State<H, T>(head, null));
		} catch (StorageException e) {
			throw new AStarException(e);
		} finally {
			lock.readLock().unlock();
		}
		// }

	}

	// AA: change visibility to public so that marking can be identified
	// directly from record
	@Override
	public State<H, T> getStoredState(Record rec) throws AStarException {
		try {
			return rec.getState(store);
		} catch (StorageException e) {
			throw new AStarException(e);
		}
	}

	@Override
	public int getEstimate(H head, long index) throws AStarException {
		FastByteArrayInputStream stream = store.getStreamForObject(index);
		try {
			synchronized (stream.getLock()) {
				return ((StorageAwareDelegate<H, T>) delegate)
						.getTailInflater().inflateEstimate(
								(StorageAwareDelegate<H, T>) delegate, head,
								stream);
			}
		} catch (IOException e) {
			throw new AStarException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <TT extends FastLowerBoundTail> void reComputeFastLowerboundTail(
			Record rec, TT tail, H head, int storedEstimate)
			throws AStarException {

		if (!tail.isExactEstimateKnown()) {
			tail.computeEstimate(delegate, head, storedEstimate);
			// The tail object may have changed. Update in storage!
			assert tail.isExactEstimateKnown();
			// synchronized (statespace) {
			SkippableOutputStream out = store.getOutputStreamForObject(rec
					.getState());
			// rewrite the tail in this outputstream
			try {
				((CachedStorageAwareDelegate<H, TT>) delegate)
						.getHeadDeflater().skip(head, out);
				synchronized (out.getLock()) {
					((CachedStorageAwareDelegate<H, TT>) delegate)
							.getTailDeflater().deflate(tail, out);
				}
			} catch (IOException e) {
				throw new AStarException(e);
			}
			// }
		}

	}

}