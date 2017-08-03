package org.rapidprom.operators.niek;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.PetrinetEdge;
import org.processmining.models.graphbased.directed.petrinet.PetrinetNode;
import org.processmining.models.graphbased.directed.petrinet.elements.Place;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.petrinet.replayresult.PNRepResult;
import org.processmining.plugins.replayer.replayresult.SyncReplayResult;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class DeterminismCalculator {
	public static double getDeterminism(final Petrinet pn, final PNRepResult alignments, final Marking initialMarking){
		double avgEnabledTransitions = 0d;
		int avgEnabledCounter = 0;

		if(alignments!=null){
			Iterator<SyncReplayResult> sync = alignments.iterator();
			while(sync.hasNext()){
				SyncReplayResult srr = sync.next();
				int processVariantCount = srr.getTraceIndex().size();
				List<Object> nodeInstances = srr.getNodeInstance();
				Multiset<Place> currentMarking = HashMultiset.create(initialMarking);
	
				Set<Transition> enabled = getEnabledVisibleTransitions(pn, currentMarking);
				int numEnabled = enabled.size();

				for(int i=0; i<nodeInstances.size(); i++){
					Object nodeInstance = nodeInstances.get(i);
					if(nodeInstance instanceof Transition){
						Transition transition = (Transition) nodeInstance;
						if(!transition.isInvisible()){
							if(numEnabled>0){
								avgEnabledTransitions = (avgEnabledCounter * avgEnabledTransitions + processVariantCount* numEnabled) / (avgEnabledCounter+processVariantCount);
								avgEnabledCounter+=processVariantCount;
							}
						}
						currentMarking = getMarkingAfterFiring(pn, currentMarking, transition);
						enabled = getEnabledVisibleTransitions(pn, currentMarking);
						numEnabled = enabled.size();
					}
				}
			}
		}
		return avgEnabledTransitions;
	}
	
	public static Set<Transition> getDirectlyEnabledSilentTransitions(final Petrinet net, final Multiset<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(t.isInvisible() && isTransitionDirectlyEnabled(net, marking, t))
				enabledTransitions.add(t);
		}
		return enabledTransitions;
	}
	
	public static Set<Transition> getEnabledVisibleTransitions(final Petrinet net, final Multiset<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(!t.isInvisible() && isTransitionEnabled(net, marking, t)){
				enabledTransitions.add(t);
			}
		}
		return enabledTransitions;
	}
	
	public static Set<Transition> getEnabledTransitions(final Petrinet net, final Multiset<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(isTransitionEnabled(net, marking, t))
				enabledTransitions.add(t);
		}
		return enabledTransitions;
	}
	
	public static boolean canReachPlace(final Set<Place> marking, final Place sourcePlace, final Petrinet net){
		if(marking.contains(sourcePlace))
			return true;
		else{
			boolean result = false;
			for(Transition t : getEnabledSilentTransitions(net, marking)){
				Set<Transition> attemptedSilents = new HashSet<Transition>();
				Set<Place> newMarking = getMarkingAfterFiring(net, marking, t);
				attemptedSilents.add(t);
				if(canReachPlace(newMarking, sourcePlace, net, attemptedSilents)){
					result = true;
					break;
				}
			}
			return result;
		}
	}
	
	public static boolean canReachPlace(final Set<Place> marking, final Place sourcePlace, final Petrinet net, final Set<Transition> attemptedSilents){
		if(marking.contains(sourcePlace))
			return true;
		else{
			boolean result = false;
			for(Transition t : getEnabledSilentTransitions(net, marking)){
				if(attemptedSilents.contains(t))
					continue;
				Set<Place> newMarking = getMarkingAfterFiring(net, marking, t);
				attemptedSilents.add(t);
				if(canReachPlace(newMarking, sourcePlace, net, new HashSet<Transition>(attemptedSilents))){
					result = true;
					break;
				}
			}
			return result;
		}
	}
	
	public static boolean canReachPlace(final Multiset<Place> marking, final Place sourcePlace, final Petrinet net){
		if(marking.contains(sourcePlace))
			return true;
		else{
			boolean result = false;
			for(Transition t : getDirectlyEnabledSilentTransitions(net, marking)){
				Set<Transition> attemptedSilents = new HashSet<Transition>();
				Multiset<Place> newMarking = getMarkingAfterFiring(net, marking, t);
				attemptedSilents.add(t);
				if(canReachPlace(newMarking, sourcePlace, net, attemptedSilents)){
					result = true;
					break;
				}
			}
			return result;
		}
	}
	
	public static boolean canReachPlace(final Multiset<Place> marking, final Place sourcePlace, final Petrinet net, final Set<Transition> attemptedSilents){
		if(marking.contains(sourcePlace))
			return true;
		else{
			boolean result = false;
			for(Transition t : getDirectlyEnabledSilentTransitions(net, marking)){
				if(attemptedSilents.contains(t))
					continue;
				Multiset<Place> newMarking = getMarkingAfterFiring(net, marking, t);
				attemptedSilents.add(t);
				if(canReachPlace(newMarking, sourcePlace, net, new HashSet<Transition>(attemptedSilents))){
					result = true;
					break;
				}
			}
			return result;
		}
	}
	
	public static boolean isTransitionEnabled(final Petrinet net, final Multiset<Place> marking, final Transition toFire){
		if(!net.getTransitions().contains(toFire))
			return false;
		for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getInEdges(toFire)){
			Place sourcePlace = (Place) e.getSource();
			if(!marking.contains(sourcePlace)){
				if(!canReachPlace(marking, sourcePlace, net))
					return false;
			}
		}
		return true;
	}
	
	public static boolean isTransitionDirectlyEnabled(final Petrinet net, final Multiset<Place> marking, final Transition toFire){
		if(!net.getTransitions().contains(toFire))
			return false;
		for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getInEdges(toFire)){
			Place sourcePlace = (Place) e.getSource();
			if(!marking.contains(sourcePlace)){
				return false;
			}
		}
		return true;
	}
	
	public static boolean isTransitionDirectlyEnabled(final Petrinet net, final Set<Place> marking, final Transition toFire){
		if(!net.getTransitions().contains(toFire))
			return false;
		for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getInEdges(toFire)){
			Place sourcePlace = (Place) e.getSource();
			return !marking.contains(sourcePlace);
		}
		return true;
	}
	
	public static boolean isTransitionEnabled(final Petrinet net, final Set<Place> marking, final Transition toFire){
		if(!net.getTransitions().contains(toFire))
			return false;
		for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getInEdges(toFire)){
			Place sourcePlace = (Place) e.getSource();
			if(!marking.contains(sourcePlace))
				return canReachPlace(marking, sourcePlace, net);
		}
		return true;
	}
	
	public static Multiset<Place> getMarkingAfterFiring(final Petrinet net, final Multiset<Place> markingBefore, final Transition toFire){
		Multiset<Place> markingAfter = HashMultiset.create();
		markingAfter.addAll(markingBefore);
		if(isTransitionEnabled(net, markingBefore, toFire)){
			for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getInEdges(toFire))
				markingAfter.remove(e.getSource());
			
			for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getOutEdges(toFire))
				markingAfter.add((Place) e.getTarget());
		}else{
			System.err.println("The provided transition "+toFire+" is  not enabled in given Petri net from given marking "+markingBefore+", marking allows "+getEnabledTransitions(net, markingBefore)+", net: "+prettyPrintPetrinet(net));
		}
		return markingAfter;
	}
	
	public static Set<Place> getMarkingAfterFiring(final Petrinet net, final Set<Place> markingBefore, final Transition toFire){
		Set<Place> markingAfter = new HashSet<Place>();
		markingAfter.addAll(markingBefore);
		if(isTransitionEnabled(net, markingBefore, toFire)){
			for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getInEdges(toFire))
				markingAfter.remove(e.getSource());
			for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getOutEdges(toFire))
				markingAfter.add((Place) e.getTarget());
		}else{
			System.err.println("The provided transition "+toFire+" is  not enabled in given Petri net from given marking "+markingBefore+", marking allows "+getEnabledTransitions(net, markingBefore)+", net: "+prettyPrintPetrinet(net));
		}
		return markingAfter;
	}
	
	public static Set<Transition> getEnabledVisibleTransitions(final Petrinet net, final Set<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(!t.isInvisible() && isTransitionEnabled(net, marking, t))
				enabledTransitions.add(t);
		}
		return enabledTransitions;
	}
	
	public static Set<Transition> getDirectlyEnabledSilentTransitions(final Petrinet net, final Set<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(t.isInvisible() && isTransitionDirectlyEnabled(net, marking, t))
				enabledTransitions.add(t);
		}
		return enabledTransitions;
	}
	
	public static Set<Transition> getEnabledSilentTransitions(final Petrinet net, final Set<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(t.isInvisible() && isTransitionEnabled(net, marking, t))
				enabledTransitions.add(t);
		}
		return enabledTransitions;
	}
	
	public static Set<Transition> getEnabledTransitions(final Petrinet net, final Set<Place> marking){
		Set<Transition> enabledTransitions = new HashSet<Transition>();
		for(Transition t : net.getTransitions()){
			if(isTransitionEnabled(net, marking, t))
				enabledTransitions.add(t);
		}
		return enabledTransitions;
	}
	
	public static String prettyPrintPetrinet(final Petrinet net){
		StringBuilder sb = new StringBuilder();
		sb.append("Transitions: ");
		sb.append(net.getTransitions());
		sb.append('\n');
		sb.append("Places:      ");
		sb.append(net.getPlaces());
		sb.append('\n');
		sb.append("Edges:        [");
		for(PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> e : net.getEdges()){
			sb.append(e.getSource());
			sb.append(" -> ");
			sb.append(e.getTarget());
			sb.append(',');
		}
		sb.append('\n');
		return sb.toString();
	}
}
