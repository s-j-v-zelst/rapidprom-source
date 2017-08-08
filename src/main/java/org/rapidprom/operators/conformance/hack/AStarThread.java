package org.rapidprom.operators.conformance.hack;

import gnu.trove.map.TObjectIntMap;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import nl.tue.astar.AStarException;
import nl.tue.astar.Delegate;
import nl.tue.astar.Head;
import nl.tue.astar.Record;
import nl.tue.astar.Tail;
import nl.tue.astar.Trace;
import nl.tue.astar.impl.State;

/**
 * Interface for computing one (or more) alignments between a trace and a model.
 * 
 * After instantiation, the sorting and priority queue ordering should be set
 * before any call to getOptimalRecord.
 * 
 * On the first call to getOptimalRecord, the AStar algorithm is used to compute
 * an optimal alignment between a model and a trace. The possible moves in every
 * state are computed using the delegate. When finished, a record is always
 * returned. If wasReliable() returns true, then the record is indeed an optimal
 * one, otherwise it is a best-guess.
 * 
 * As long as wasReliable() returns true, more alignments can be computed by
 * repeated calls to getOptimalRecord(C, i), where i is the cost of the previous
 * optimal record. This way, the algorithm quarantees that all states which are
 * on a shortest path from the source to the target node will be visisted (note
 * that this does not imply that all paths will be found).
 * 
 * 
 * @author bfvdonge
 * 
 * @param <H>
 * @param <T>
 */
public interface AStarThread<H extends Head, T extends Tail> {

	public static enum Type {
		PLAIN, WEIGHTED_STATIC, WEIGHTED_DYNAMIC
	};

	public static class CPUEfficient<H extends Head, T extends Tail> extends
			AbstractAStarThread<H, T> {

		public CPUEfficient(Delegate<H, T> delegate, TObjectIntMap<H> head2int,
				List<State<H, T>> stateList, H initialHead, Trace trace,
				int maxStates) throws AStarException {
			super(delegate, trace, maxStates,
					new JavaCollectionStorageHandler<H, T>(delegate, head2int,
							stateList));
			initializeQueue(initialHead);
		}
	}

	public static class MemoryEfficient<H extends Head, T extends Tail> extends
			AbstractAStarThread<H, T> {

		public MemoryEfficient(MemoryEfficientAStarAlgorithm<H, T> algorithm,
				H initialHead, Trace trace, int maxStates)
				throws AStarException {
			super(algorithm.getDelegate(), trace, maxStates,
					new MemoryEfficientStorageHandler<H, T>(algorithm));
			initializeQueue(initialHead);
		}
	}

	public final static int NOMOVE = Integer.MIN_VALUE;

	/**
	 * Returns the trace for which this AStarThread was instantiated.
	 * 
	 * @return
	 */
	public Trace getTrace();

	/**
	 * returns the delegate used for determining the possible moves during
	 * replay.
	 * 
	 * @return
	 */
	public Delegate<H, T> getDelegate();

	/**
	 * Enumeration to set the sorting of moves. When computing an alignment, it
	 * is generally possible to sort the moves in between two synchronous moves.
	 * If LOGMOVEFIST is used, then no log-move will ever succeed a model-Move.
	 * If MOVEMODELFIRST is used, then no model-move will ever succeed a
	 * log-move.
	 * 
	 * @author bfvdonge
	 * 
	 */
	public static enum ASynchronousMoveSorting {
		NONE, LOGMOVEFIRST, MODELMOVEFIRST;
	}

	/**
	 * Returns a Record for an optimal alignment. If no optimal alignment
	 * exists, or if none is found in time, a "best guess" is returned. To check
	 * whether the returned alignment is optimal, the method isReliable() can be
	 * used.
	 * 
	 * To make sure you investigated the entire graph from which the (a) final
	 * node is reachable, keep calling the method getOptimalRecord(c, stopAt)
	 * with the stopAt cost equal to the total cost of the first record. When
	 * doing so until the first unreliable result, the entire search space is
	 * covered.
	 * 
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public Record getOptimalRecord() throws AStarException;

	/**
	 * Returns a Record for an optimal alignment. If no optimal alignment
	 * exists, if none is found in time or if the cost of an optimal alignment
	 * is higher than stopAt a "best guess" is returned. To check whether the
	 * returned alignment is optimal, the method isReliable() can be used.
	 * 
	 * To make sure you investigated the entire graph from which the (a) final
	 * node is reachable, keep calling the method getOptimalRecord(c, stopAt)
	 * with the stopAt cost equal to the total cost of the first record. When
	 * doing so until the first unreliable result, the entire search space is
	 * covered.
	 * 
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public Record getOptimalRecord(int stopAt)
			throws AStarException;

	/**
	 * Returns the number of nodes queueud. This is at least the number of nodes
	 * visisted, but can be higher due to updates.
	 * 
	 * @return
	 */
	public int getQueuedStateCount();

	/**
	 * Returns the number of arcs that were traversed in the statespace.
	 * 
	 * @return
	 */
	public int getTraversedArcCount();

	/**
	 * Returns the number of visited states.
	 * 
	 * @return
	 */
	public int getVisitedStateCount();

	/**
	 * By default, depth is preferred. With a call to preferBreadth, this can be
	 * changed. The internal priority queue is changed, such that if depth is
	 * preferred, then records with the same total costs are sorted according to
	 * their cost so far, with the highest cost first. If breadth is preferred,
	 * then records with the same total costs are sorted with lowest cost so far
	 * first.
	 * 
	 * This call should be placed before any call to any of the run methods.
	 * 
	 * @param preferBreath
	 */
	public void setPreferBreadth(boolean preferBreadth);

	public boolean getPreferBreadth();

	/**
	 * When aligning a trace with a model, any moves between two synchronous
	 * moves can be sorted.
	 * 
	 * When sorting is set to LOGMOVEFIRST, then no logMove can ever follow a
	 * modelMove. If sorting is set to ModelMoveFirst, then no modelMove can
	 * ever follow a logMove.
	 * 
	 * The default is LOGMOVEFIRST;
	 * 
	 * @param sorting
	 */
	public void setASynchronousMoveSorting(ASynchronousMoveSorting sorting);

	public ASynchronousMoveSorting getSorting();

	/**
	 * After a call to run(), this method returns true if the returned Record
	 * yields the optimal result. If this method returns false, then the
	 * returned Record was subOptimal, for example because the maximum state
	 * count was reached, of because no solution was found. The Record returned
	 * by the previous call to run should be considered as a best guess.
	 * 
	 * @return
	 */
	public boolean wasReliable();

	/**
	 * Sets the type of the AStar algorithm. Can be changed during execution,
	 * however if type is set to any weighted variant, the epsilon and expected
	 * length should be set.
	 * 
	 * @param type
	 */
	public void setType(Type type);

	/**
	 * Returns the type of AStar used
	 * 
	 * @return
	 */
	public Type getType();

	/**
	 * Set epsilon for the weighted variants of A Star
	 * 
	 * @param epsilon
	 */
	public void setEpsilon(double epsilon);

	/**
	 * Set expected length for the dynamic weighted variant of A Star
	 * 
	 * @param length
	 */
	public void setExpectedLength(int length);
}