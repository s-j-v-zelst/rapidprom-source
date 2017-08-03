package org.rapidprom.operators.niek;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import no.uib.cipr.matrix.DenseMatrix;

import org.apache.commons.collections15.iterators.ArrayIterator;
import org.deckfour.xes.classification.XEventClass;
import org.processmining.plugins.InductiveMiner.MultiSet;
import org.processmining.plugins.InductiveMiner.graphs.Graph;
import org.processmining.plugins.InductiveMiner.graphs.GraphFactory;

public class Dfpg {
	private Graph<XEventClass> directlyFollowsGraph;
	private Graph<XEventClass> directlyPrecedesGraph;

	private final MultiSet<XEventClass> startActivities;
	private final MultiSet<XEventClass> endActivities;
	private MultiSet<XEventClass> activitiesCounts;

	public Dfpg() {
		this(1);
	}

	public Dfpg(int initialSize) {
		directlyFollowsGraph = GraphFactory.create(XEventClass.class, initialSize);
		directlyPrecedesGraph = GraphFactory.create(XEventClass.class, initialSize);
		startActivities = new MultiSet<>();
		endActivities = new MultiSet<>();
		activitiesCounts = new MultiSet<>();
	}

	public Dfpg(Graph<XEventClass> directlyFollowsGraph, Graph<XEventClass> directlyPrecedesGraph) {
		this.directlyFollowsGraph = directlyFollowsGraph;
		this.directlyPrecedesGraph = directlyPrecedesGraph;
		startActivities = new MultiSet<>();
		endActivities = new MultiSet<>();
		activitiesCounts = new MultiSet<>();
	}

	public Dfpg(final Graph<XEventClass> directlyFollowsGraph, final Graph<XEventClass> directlyPrecedesGraph,
			final MultiSet<XEventClass> startActivities, final MultiSet<XEventClass> endActivities, final MultiSet<XEventClass> activities) {
		this.directlyFollowsGraph = directlyFollowsGraph;
		this.directlyPrecedesGraph = directlyPrecedesGraph;

		this.startActivities = startActivities;
		this.endActivities = endActivities;
		this.activitiesCounts = activities;		
	}

	public void addActivity(XEventClass activity) {
		directlyFollowsGraph.addVertex(activity);
		directlyPrecedesGraph.addVertex(activity);
		activitiesCounts.add(activity);
	}

	public Graph<XEventClass> getDirectlyFollowsGraph() {
		return directlyFollowsGraph;
	}
	
	public Graph<XEventClass> getDirectlyPrecedesGraph() {
		return directlyPrecedesGraph;
	}
	
	public void setDirectlyFollowsGraph(Graph<XEventClass> directlyFollowsGraph) {
		this.directlyFollowsGraph = directlyFollowsGraph;
	}

	public Iterable<XEventClass> getActivities() {
		return new Iterable<XEventClass>() {
			public Iterator<XEventClass> iterator() {
				return new ArrayIterator<XEventClass>(directlyFollowsGraph.getVertices());
			}
		};

	}
	
	public Map<Integer, XEventClass> getMappingFromIDToXEventClass(){
		Iterator<XEventClass> activities = getActivities().iterator();
		Map<Integer, XEventClass> mapping = new HashMap<Integer, XEventClass>();
		int i =0;
		while(activities.hasNext()){
			XEventClass xec = activities.next();
			mapping.put(i, xec);
			i++;
		}
		return mapping;
	}

	public void setActivitiesCounts(MultiSet<XEventClass> counts){
		this.activitiesCounts = counts;
	}
	
	public MultiSet<XEventClass> getActivitiesCounts(){
		return activitiesCounts;
	}
	
	public MultiSet<XEventClass> getStartActivities() {
		return startActivities;
	}

	public MultiSet<XEventClass> getEndActivities() {
		return endActivities;
	}

	public void addEdge(final XEventClass source, final XEventClass target, final long cardinality) {
		//addActivity(source);
		//addActivity(target);
		directlyFollowsGraph.addEdge(source, target, cardinality);
		directlyPrecedesGraph.addEdge(target, source, cardinality);
	}

	public void addStartActivity(XEventClass activity, long cardinality) {
		//addActivity(activity);
		startActivities.add(activity, cardinality);
	}

	public void addEndActivity(XEventClass activity, long cardinality) {
		//addActivity(activity);
		endActivities.add(activity, cardinality);
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		for (long edgeIndex : directlyFollowsGraph.getEdges()) {
			result.append(directlyFollowsGraph.getEdgeSource(edgeIndex));
			result.append("->");
			result.append(directlyFollowsGraph.getEdgeTargetIndex(edgeIndex));
			result.append(", ");
		}
		return result.toString();
	}
	
	public DenseMatrix getDpgMatrix(){
		Map<Integer, XEventClass> mapping = getMappingFromIDToXEventClass();
		int numUniqueActivities = mapping.keySet().size();
		double[][] values = new double[numUniqueActivities][numUniqueActivities];
		for(int i : mapping.keySet()){
			long cardi = activitiesCounts.getCardinalityOf(mapping.get(i));
			for(int j : mapping.keySet()){
				long cardj = activitiesCounts.getCardinalityOf(mapping.get(j));
				Long weight = directlyFollowsGraph.getEdgeWeight(i, j);
				Double weightAsDouble = weight == null ? 0d : (double) weight;
				if(weightAsDouble>0 && cardj>0)
					weightAsDouble /= cardj;
				values[i][j] = weightAsDouble;
				weight = directlyFollowsGraph.getEdgeWeight(j, i);
				weightAsDouble = weight == null ? 0d : (double) weight;
				if(weightAsDouble>0 && cardi>0)
					weightAsDouble /= cardi;
				values[i][j]= Math.max(values[i][j], weightAsDouble);
			}
		}
		printArray(values);
		DenseMatrix dMatrix = new DenseMatrix(values);
		return dMatrix;
	}
	
	public static void printArray(double matrix[][]) {
	    for (double[] row : matrix) 
	        System.out.println(Arrays.toString(row));       
	}
	
	public DenseMatrix getDfgMatrix(){
		Map<Integer, XEventClass> mapping = getMappingFromIDToXEventClass();
		int numUniqueActivities = mapping.keySet().size();
		double[][] values = new double[numUniqueActivities][numUniqueActivities];
		for(int i : mapping.keySet()){
			long cardi = activitiesCounts.getCardinalityOf(mapping.get(i));
			for(int j : mapping.keySet()){
				long cardj = activitiesCounts.getCardinalityOf(mapping.get(j));
				Long weight = directlyPrecedesGraph.getEdgeWeight(i, j);
				Double weightAsDouble = weight == null ? 0d : (double) weight;
				if(weightAsDouble>0 && cardj>0)
					weightAsDouble /= cardj;
				values[i][j] = weightAsDouble;
				weight = directlyPrecedesGraph.getEdgeWeight(j, i);
				weightAsDouble = weight == null ? 0d : (double) weight;
				if(weightAsDouble>0 && cardi>0)
					weightAsDouble /= cardi;
				values[i][j]= Math.max(values[i][j], weightAsDouble);
			}
		}
	
		DenseMatrix dMatrix = new DenseMatrix(values);
		return dMatrix;
	}
	
	public static String padRight(String s, int n) {
	     return String.format("%1$-" + n + "s", s);  
	}
}