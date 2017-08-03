package org.rapidprom.operators.conformance.hack;

import gnu.trove.TIntCollection;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.tue.astar.AStarException;
import nl.tue.astar.AStarObserver;
import nl.tue.astar.Delegate;
import nl.tue.astar.FastLowerBoundTail;
import nl.tue.astar.Head;
import nl.tue.astar.Record;
import nl.tue.astar.Tail;
import nl.tue.astar.Trace;
import nl.tue.astar.impl.State;
import nl.tue.astar.util.BreadthFirstFastLookupPriorityQueue;
import nl.tue.astar.util.DepthFirstFastLookupPriorityQueue;
import nl.tue.astar.util.FastLookupPriorityQueue;
import nl.tue.astar.util.RandomFastLookupPriorityQueue;

public abstract class AbstractAStarThread<H extends Head, T extends Tail>
		implements ObservableAStarThread<H, T> {

	public static enum QueueingModel {
		DEPTHFIRST, BREADTHFIRST, RANDOM
	};

	/**
	 * The storageHandler handles the storing and retrieving of elements from
	 * the statespace searched by the AStar algorithm
	 * 
	 * @author bfvdonge
	 * 
	 * @param <H>
	 * @param <T>
	 */
	public static interface StorageHandler<H extends Head, T extends Tail> {

		/**
		 * return the estimate of the tail for the given head. The head is
		 * stored at the given index.
		 * 
		 * @param head
		 * @param index
		 * @return
		 * @throws AStarException
		 */
		public int getEstimate(H head, long index) throws AStarException;

		public void storeStateForRecord(State<H, T> state, Record newRec)
				throws AStarException;

		public long getIndexOf(H head) throws AStarException;

		public State<H, T> getStoredState(Record rec) throws AStarException;

		/**
		 * implementations may assume that TT == T
		 * 
		 * @param <TT>
		 * @param rec
		 * @param tail
		 * @param head
		 * @param storedEstimate
		 * @throws AStarException
		 */
		public <TT extends FastLowerBoundTail> void reComputeFastLowerboundTail(
				Record rec, TT tail, H head, int storedEstimate)
				throws AStarException;

	}

	protected FastLookupPriorityQueue queue;
	protected final Trace trace;
	protected final int maxStates;
	protected int queuedStateCount = 0;
	protected int traversedArcCount = 0;
	protected int computedEstimateCount = 0;
	protected final Delegate<H, T> delegate;
	protected int poll = 0;

	protected static int i = 0;
	protected ASynchronousMoveSorting sorting;
	protected boolean reliable;

	protected final TLongSet considered;

	protected List<AStarObserver> observers = new ArrayList<AStarObserver>(4);
	protected final StorageHandler<H, T> storageHandler;
	protected Type type = Type.PLAIN;
	protected double epsilon = 0;
	protected double expectedLength = 10.;

	/**
	 * any implementation should, after calling this constructor, call
	 * initializeQueue(initialHead);
	 * 
	 * @param delegate
	 * @param trace
	 * @param maxStates
	 * @param storageHandler
	 */
	public AbstractAStarThread(Delegate<H, T> delegate, Trace trace,
			int maxStates, StorageHandler<H, T> storageHandler) {
		this.delegate = delegate;
		this.trace = trace;
		this.maxStates = maxStates;
		this.storageHandler = storageHandler;
		this.queue = new BreadthFirstFastLookupPriorityQueue(1000);
		this.sorting = ASynchronousMoveSorting.MODELMOVEFIRST;
		this.considered = new TLongHashSet(1000, 0.5f, -2l);

	}

	public Trace getTrace() {
		return trace;
	}

	public Delegate<H, T> getDelegate() {
		return delegate;
	}

	/**
	 * Use setQueueingModel() or setQueue instead;
	 */
	@Deprecated
	public void setPreferBreadth(boolean preferBreadth) {
		setQueueingModel(preferBreadth ? QueueingModel.BREADTHFIRST
				: QueueingModel.DEPTHFIRST);
	}

	public void setQueueingModel(QueueingModel model) {
		switch (model) {
		case BREADTHFIRST:
			setQueue(new BreadthFirstFastLookupPriorityQueue(1000));
			break;
		case DEPTHFIRST:
			setQueue(new DepthFirstFastLookupPriorityQueue(1000));
			break;
		case RANDOM:
			setQueue(new RandomFastLookupPriorityQueue(1000, 0.5));
			break;
		}
	}

	public void setQueue(FastLookupPriorityQueue newQueue) {
		if (newQueue.size() > 1) {
			throw new UnsupportedOperationException(
					"Cannot change the queue after elements have been inserted.");
		}
		FastLookupPriorityQueue oldQueue = this.queue;

		this.queue = newQueue;

		if (oldQueue.size() == 1) {
			Record d = oldQueue.peek();
			newQueue.add(d);
		}
	}

	public void addObserver(AStarObserver observer) {
		observers.add(observer);
		observer.initialNodeCreated(queue.peek());
	}

	public void removeObserver(AStarObserver observer) {
		observers.remove(observer);
	}
	
	public void close(){
		for(AStarObserver observer : observers){
			observer.close();
		}
		observers.clear();
	}

	public boolean getPreferBreadth() {
		return this.queue instanceof BreadthFirstFastLookupPriorityQueue;
	}

	public void setASynchronousMoveSorting(ASynchronousMoveSorting sorting) {
		this.sorting = sorting;
	}

	public ASynchronousMoveSorting getSorting() {
		return sorting;
	}

	public Record getOptimalRecord() throws AStarException {
		return getOptimalRecord(Integer.MAX_VALUE);
	}

	protected Record poll() {
		Record node = queue.poll();
		return node;
	}

	/**
	 * gets the optimal record. The search stops and returns the best prefix so
	 * far if either the stopAt value is reached in terms of cost, or if the
	 * timeLimit in seconds is reached. If the timelimit is negative, then time
	 * is unlimited.
	 * 
	 * @param c
	 * @param stopAt
	 * @param timeLimit
	 * @return
	 * @throws AStarException
	 */
	public Record getOptimalRecord(final int stopAt) throws AStarException {

		State<H, T> state;
		Record rec = null;
		H head = null;
		T tail = null;

		queue.setMaxCost(stopAt);
		
		while (!queue.isEmpty()) {
			if(Thread.currentThread().isInterrupted()){
				throw new AStarException("Time-out");
			}
			rec = poll();
			poll++;
			try {
				state = storageHandler.getStoredState(rec);
			} catch (Exception e) {
				throw new AStarException(e);
			}
			head = state.getHead();
			tail = state.getTail();

			if (head.isFinal(delegate)) {
				this.reliable = true;
				for (AStarObserver observer : observers) {
					observer.finalNodeFound(rec);
				}
				return rec;
			}

			if (poll >= maxStates || rec.getTotalCost() > stopAt) {
				// unreliable, best guess:
				this.reliable = false;
				for (AStarObserver observer : observers) {
					observer.stoppedUnreliablyAt(rec);
				}
				return rec;
			}

			processMovesForRecord(rec, head, tail, stopAt);

		}

		this.reliable = false;
		for (AStarObserver observer : observers) {
			observer.stoppedUnreliablyAt(rec);
		}
		return rec;
	}

	protected void processMovesForRecord(Record rec, H head, T tail,
			int stopAt) throws AStarException {

		double storedEstimate = rec.getEstimatedRemainingCost();

		if (!rec.isExactEstimate()) {
			int recEstimate = (int) storedEstimate;
			// the stored estimate is an inexact estimate, hence it is
			// integer.
			assert recEstimate == storedEstimate;

			// rec.getEstimatedRemainingCost() >= 0
			// && tail instanceof FastLowerBoundTail) {

			// The estimate in this record is not exact yet. We need to
			// get the exact estimate from the tail and requeue the record if
			// necessary

			// we have a fast-lowerbound tail.
			// recompute and reschedule if necessary
			FastLowerBoundTail ftail = (FastLowerBoundTail) tail;
			boolean wasExact = ftail.isExactEstimateKnown();

			if (!wasExact) {
				for (AStarObserver observer : observers) {
					observer.estimateComputed(head);
				}

				computedEstimateCount++;
				storageHandler.reComputeFastLowerboundTail(rec, ftail, head,
						recEstimate);
			}

			int tailEstimate = tail.getEstimatedCosts(delegate, head);

			// what if tail now indicates impossible termination
			if (!tail.canComplete()) {
				// skip this state.
				// no moves need to be processed.
				return;
			}

			assert storedEstimate <= tailEstimate;

			if (recEstimate == tailEstimate) {
				if (type == Type.PLAIN) {
					// for the plain A Star implementation, the actual
					// exact was set correctly, hence we can continue with this
					// record. No need to requeue
					rec.setEstimatedRemainingCost(
							rec.getEstimatedRemainingCost(), true);
				} else {
					// we now have an exact estimate. Requeue the node with this
					// exact estimate which may be adjusted based on Epsilon,
					// hence reqeueuing needs to be done.
					reQueueWithExactEstimate(rec, tailEstimate);
					return;
				}
			} else if (recEstimate < tailEstimate) {
				// the tail's true estimate is higher than the fast one stored
				// earlier. Re-queue the record with the exact estimate
				reQueueWithExactEstimate(rec, tailEstimate);
				return;
			}
		}
		processMovesForRecordWithUpToDateTail(rec, head, tail, stopAt);
	}

	private void reQueueWithExactEstimate(Record rec, int tailEstimate) {

		// exact estimate known
		setExactEstimateForRecord(rec, tailEstimate);

		// reduce the pollCount
		poll--;

		// re-queue the state
		if (rec.getTotalCost() > queue.getMaxCost()) {
			// new record has guaranteed higher cost than the queue's
			// maxcost, this state needs no further investigation.
			setConsidered(rec);
		} else if (queue.add(rec)) {
			queuedStateCount++;
		}

		// edge from oldRec to newRec traversed.
		for (AStarObserver observer : observers) {
			observer.edgeTraversed(rec, rec);
		}
	}

	private void setExactEstimateForRecord(Record rec, int estimate) {
		switch (type) {
		case PLAIN:
			rec.setEstimatedRemainingCost(estimate, true);
			break;
		case WEIGHTED_STATIC:
			rec.setEstimatedRemainingCost((1 + epsilon) * estimate, true);
			break;
		case WEIGHTED_DYNAMIC:
			if (rec.getBacktraceSize() < expectedLength) {
				rec.setEstimatedRemainingCost(
						(1 + epsilon
								* (1 - rec.getBacktraceSize() / expectedLength))
								* estimate, true);
			} else {
				rec.setEstimatedRemainingCost(estimate, true);
			}
			break;
		}
	}

	protected void processMovesForRecordWithUpToDateTail(Record rec, H head,
			T tail, int stopAt)
			throws AStarException {

		setConsidered(rec);

		// move model only
		TIntList enabled = head.getModelMoves(rec, delegate);

		TIntCollection nextEvents = rec.getNextEvents(delegate, trace);
		TIntIterator evtIt = nextEvents.iterator();
		int activity = NOMOVE;

		while (evtIt.hasNext()) {
			if(Thread.currentThread().isInterrupted()){
				throw new AStarException("Time-out");
			}
			int nextEvent = evtIt.next();

			TIntList ml = null;

			// move both log and model synchronously;
			activity = trace.get(nextEvent);
			ml = head.getSynchronousMoves(rec, delegate, enabled, activity);
			TIntIterator it = ml.iterator();
			while (it.hasNext()) {
				if(Thread.currentThread().isInterrupted()){
					throw new AStarException("Time-out");
				}
				processMove(head, tail, rec, it.next(), nextEvent, activity);
			}

			// sorting == ASynchronousMoveSorting.LOGMOVEFIRST implies
			// logMove only after initial move, sync move or log move.
			if (isValidMoveOnLog(rec, nextEvent, activity, enabled, ml)) {
				// allow move on log only if the previous move was
				// 1) the initial move (rec.getPredecessor() == null
				// 2) a synchronous move
				// 3) a log move.
				processMove(head, tail, rec, NOMOVE, nextEvent, activity);
			}
		}

		// sorting == ASynchronousMoveSorting.MODELMOVEFIRST implies
		// modelMove only after initial move, sync move or model move.
		if (isValidMoveOnModel(rec, nextEvents, activity, enabled)) {
			// allow move on model only if previous move was:
			// 1) the initial move (rec.getPredecessor() == null
			// 2) a synchronous move
			// 3) a move on model.
			TIntIterator it = enabled.iterator();
			while (it.hasNext()) {
				if(Thread.currentThread().isInterrupted()){
					throw new AStarException("killed");
				}
				// move model
				processMove(head, tail, rec, it.next(), NOMOVE, NOMOVE);
			}
		}
	}

	protected void setConsidered(Record record) {
		considered.add(record.getState());
		for (AStarObserver observer : observers) {
			observer.nodeVisited(record);
		}
	}

	protected boolean isValidMoveOnModel(Record rec, TIntCollection nextEvents,
			int activity, TIntList modelMoves) {
		return sorting != ASynchronousMoveSorting.MODELMOVEFIRST
				|| (rec.getPredecessor() == null || rec.getModelMove() != NOMOVE);
	}

	protected boolean isValidMoveOnLog(Record rec, int nextEvent, int activity,
			TIntList modelMoves, TIntList syncMoves) {
		return sorting != ASynchronousMoveSorting.LOGMOVEFIRST
				|| (rec.getPredecessor() == null || rec.getMovedEvent() != NOMOVE);
	}

	public boolean wasReliable() {
		return reliable;
	}

	protected void processMove(H head, T tail, Record rec, int modelMove,
			int movedEvent, int activity) throws AStarException {
		// First, construct the next head from the old head
		final H newHead = computeNextHead(rec, head, modelMove, movedEvent,
				activity);
		final long index;
		try {
			index = storageHandler.getIndexOf(newHead);
		} catch (Exception e) {
			throw new AStarException(e);
		}

		// create a record for this new head
		final Record newRec = rec.getNextRecord(delegate, trace, newHead,
				index, modelMove, movedEvent, activity);
		traversedArcCount++;

		newRec.setEstimatedRemainingCost(ESTIMATEIRRELEVANT, true);

		final int c;
		if (!considered.contains(index)) {
			Record r = queue.contains(newRec);
			// if r!=null then there is a record in the queue pointing to this
			// state.
			if (r != null) {
				// We reached the state at index before, check the costs and
				// get the estimate from the previous time. Note that we CANNOT
				// get a new estimate from the tail since this would invalidate
				// the ordering relation in the priorityqueue
				c = r.getCostSoFar();
				newRec.setEstimatedRemainingCost(r.getEstimatedRemainingCost(),
						r.isExactEstimate());
			} else {
				c = -1;
			}

		} else {
			// The state at index was visited before, hence the estimate is
			// irrelevant
			c = 0;
		}
		if (c >= 0 && c <= newRec.getCostSoFar()) {

			// Either we visisted this state before,
			// or a record with the same state and lower (or equal) cost so far
			// exists.
			// this implies that newState was already fully present in the
			// statespace

			assert index >= 0;

			// edge from oldRec to newRec traversed.
			for (AStarObserver observer : observers) {
				observer.edgeTraversed(rec, newRec);
			}

			return;
		}

		final T newTail;
		if (index >= 0) {
			if (newRec.getEstimatedRemainingCost() == ESTIMATEIRRELEVANT) {
				// so far, we did the cheap stuff, now get the estimate. If no
				// estimate exists, it will be computed.
				int h;
				try {
					h = storageHandler.getEstimate(newHead, index);
				} catch (Exception e) {
					throw new AStarException(e);
				}
				if (h < 0) {
					// fast estimate of the actual estimate, not exact yet.
					newRec.setEstimatedRemainingCost(-h - 1, false);
				} else {
					// exact estimate computed.
					setExactEstimateForRecord(newRec, h);
				}

			}

			if (newRec.getTotalCost() > queue.getMaxCost()) {
				// new record has guaranteed higher cost than the queue's
				// maxcost,
				// this state needs no further investigation.
				// However, it cannot be marked as considered, as it may be
				// reached again
				// through a shorter path.
			} else if (queue.add(newRec)) {
				queuedStateCount++;
			}

			assert newRec.getState() == index;

			// edge from oldRec to newRec traversed.
			for (AStarObserver observer : observers) {
				observer.edgeTraversed(rec, newRec);
			}
			return;

		}

		// the statespace doesn't contain a corresponding state, hence we need
		// to compute the tail.
		newTail = computeNewTail(newRec, tail, newHead, modelMove, movedEvent,
				activity);

		if (!newTail.canComplete()) {
			return;
		}

		// Check if the head is in the store and add if it isn't.
		final State<H, T> newState = new State<H, T>(newHead, newTail);

		try {
			storageHandler.storeStateForRecord(newState, newRec);
		} catch (Exception e) {
			throw new AStarException(e);
		}

		// State<H, T> ret = store.getObject(r.index);
		// if (!ret.equals(newState)) {
		// System.err.println("Retrieval error");
		// }

		// assert (r.isNew);
		if (newRec.getTotalCost() > queue.getMaxCost()) {
			// new record has guaranteed higher cost than the queue's
			// maxcost,
			// this state needs no further investigation.
			// However, it cannot be marked as considered, as it may be
			// reached again
			// through a shorter path.
		} else if (queue.add(newRec)) {
			queuedStateCount++;
		}

		// edge from oldRec to newRec traversed.
		for (AStarObserver observer : observers) {
			observer.edgeTraversed(rec, newRec);
		}

	}

	@SuppressWarnings("unchecked")
	protected H computeNextHead(Record rec, H head, int modelMove,
			int movedEvent, int activity) {
		return (H) head.getNextHead(rec, delegate, modelMove, movedEvent,
				activity);
	}

	@SuppressWarnings("unchecked")
	protected T computeNewTail(Record newRec, T tail, H newHead, int modelMove,
			int movedEvent, int activity) {

		T newTail = (T) tail.getNextTail(delegate, newHead, modelMove,
				movedEvent, activity);

		if (newTail instanceof FastLowerBoundTail) {
			FastLowerBoundTail ftail = (FastLowerBoundTail) newTail;
			if (!ftail.isExactEstimateKnown()) {
				// exact estimate is not known
				// if the estimate would cause this tail to be queued in front,
				// compute the exact estimate
				int est = ftail.getEstimatedCosts(delegate, newHead);
				if (queue.isEmpty()
						|| queue.peek().getTotalCost() >= newRec.getCostSoFar()
								+ est) {
					// new record is going to be at the head of the queue.
					// compute the estimate and fall through
					ftail.computeEstimate(delegate, newHead, est);
				} else {
					// set the estimated cost to the stored estimated cost
					// without
					// recomputing the estimate.
					newRec.setEstimatedRemainingCost(est, false);
					// do not update the computedEstimateCount
					// and do not notify the observers.
					return newTail;
				}
			}
		}

		// Here, the actual, reliable estimate is set.
		setExactEstimateForRecord(newRec,
				newTail.getEstimatedCosts(delegate, newHead));

		for (AStarObserver observer : observers) {
			observer.estimateComputed(newHead);
		}
		computedEstimateCount++;

		return newTail;
	}

	protected void initializeQueue(H head) throws AStarException {
		// time = System.currentTimeMillis();
		// First, find the location of head
		final long index = storageHandler.getIndexOf(head);
		// note that the contains method may give false negatives. However,
		// it is assumed to be more expensive to synchronize on (algorithm) than
		// to
		// just recompute the tail.

		// create a record for this new head
		final Record newRec = delegate.createInitialRecord(head, trace);

		if (index < 0) {
			// the statespace doesn't contain a corresponding state
			final T tail = (T) delegate.createInitialTail(head);
			final State<H, T> newState = new State<H, T>(head, tail);

			storageHandler.storeStateForRecord(newState, newRec);

		} else {
			newRec.setState(index);
		}
		if (newRec.getTotalCost() > queue.getMaxCost()) {
			// new record has guaranteed higher cost than the queue's
			// maxcost,
			// this state needs no further investigation.
			setConsidered(newRec);
		} else if (queue.add(newRec)) {
			queuedStateCount++;
		}

	}

	public int getVisitedStateCount() {
		return poll;
	}

	public int getQueuedStateCount() {
		return queuedStateCount;
	}

	public int getTraversedArcCount() {
		return traversedArcCount;
	}

	public int getComputedEstimateCount() {
		return computedEstimateCount;
	}

	public String toString() {
		return queue.size() + ":" + queue.toString();
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Returns the type of AStar used
	 * 
	 * @return
	 */
	public Type getType() {
		return type;
	}

	/**
	 * Set epsilon for the weighted variants of A Star
	 * 
	 * @param epsilon
	 */
	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;

	}

	/**
	 * Set expected length for the dynamic weighted variant of A Star
	 * 
	 * @param length
	 */
	public void setExpectedLength(int length) {
		this.expectedLength = length;
	}

}