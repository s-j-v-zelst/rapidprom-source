package org.rapidprom.operators.conformance.hack;

import gnu.trove.map.TObjectIntMap;

import java.util.List;

import nl.tue.astar.AStarException;
import nl.tue.astar.Delegate;
import nl.tue.astar.FastLowerBoundTail;
import nl.tue.astar.Head;
import nl.tue.astar.Record;
import nl.tue.astar.Tail;
import nl.tue.astar.impl.State;

public class JavaCollectionStorageHandler<H extends Head, T extends Tail>
		implements AbstractAStarThread.StorageHandler<H, T> {

	protected final TObjectIntMap<H> head2int;
	protected final List<State<H, T>> stateList;
	private final Delegate<H, T> delegate;

	public JavaCollectionStorageHandler(Delegate<H, T> delegate,
			TObjectIntMap<H> head2int, List<State<H, T>> stateList) {

		this.delegate = delegate;
		this.head2int = head2int;
		this.stateList = stateList;
		// get the index where initialHead is stored
		// thread.initializeQueue(initialHead);
	}

	public T getStoredTail(T tail, long index, int modelMove, int movedEvent,
			int logMove) {
		return stateList.get((int) index).getTail();
	}

	public void storeStateForRecord(State<H, T> state, Record newRec) {
		synchronized (head2int) {
			synchronized (stateList) {
				int newIndex = stateList.size() + 1;
				stateList.add(state);
				head2int.put(state.getHead(), newIndex);
				newRec.setState(newIndex - 1);
			}
		}
	}

	public long getIndexOf(H head) {
		synchronized (head2int) {
			synchronized (stateList) {
				return head2int.get(head) - 1;
			}
		}
	}

	public State<H, T> getStoredState(Record rec) {
		return stateList.get((int) rec.getState());
	}

	@Override
	public int getEstimate(H head, long index) {
		T tail = stateList.get((int) index).getTail();
		if (tail instanceof FastLowerBoundTail) {
			if (!((FastLowerBoundTail) tail).isExactEstimateKnown()) {
				return -tail.getEstimatedCosts(delegate, head) - 1;
			}
		}
		return tail.getEstimatedCosts(delegate, head);
	}

	@Override
	public <TT extends FastLowerBoundTail> void reComputeFastLowerboundTail(
			Record rec, TT tail, H head, int storedEstimate)
			throws AStarException {
		tail.computeEstimate(delegate, head, storedEstimate);
	}

}
