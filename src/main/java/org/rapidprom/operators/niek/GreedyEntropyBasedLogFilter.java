package org.rapidprom.operators.niek;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginVariant;
import org.processmining.plugins.InductiveMiner.graphs.Graph;

import com.google.common.math.DoubleMath;


@Plugin(
		name = "Filter Log using Entropy (Greedy)", 
		parameterLabels = {"Input Log"}, 
	    returnLabels = {"Logs"}, 
	    returnTypes = { List.class }
		)
public class GreedyEntropyBasedLogFilter{
	private XLog log;
	private Map<Integer, Set<String>> bestProjectionForSize;
	private Set<Set<String>> hashset;
	
	public static XLog projectLogOnEventNames(final XLog log, final Set<String> eventSet){
		XLog logClone = (XLog) log.clone();
		Set<XTrace> traceRemoveSet = new HashSet<XTrace>();
		for(XTrace trace : logClone){
			Set<XEvent> eventRemoveSet = new HashSet<XEvent>();
			for(XEvent event : trace){
				if(!eventSet.contains(event.getAttributes().get("concept:name").toString()))
					eventRemoveSet.add(event);						
			}
			for(XEvent removeEvent : eventRemoveSet)
				trace.remove(removeEvent);
			if(trace.size()==0)
				traceRemoveSet.add(trace);
		}
		for(XTrace removeTrace : traceRemoveSet)
			logClone.remove(removeTrace);
		return logClone;
	}
	
	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "N. Tax", email = "n.tax@tue.nl")
	@PluginVariant(variantLabel = "Filter Log using Entropy (Greedy)", requiredParameterLabels = {0})
	public List<Set<String>> getProjections(PluginContext context, XLog log) {
		this.log = log;
		bestProjectionForSize = new HashMap<Integer, Set<String>>();
		hashset = new HashSet<Set<String>>();
		
		// build initial set
		XEventClasses info = XLogInfoFactory.createLogInfo(log, new XEventNameClassifier()).getEventClasses();
		Set<String> sizeAllSet = new HashSet<String>();
		for(int i=0; i<info.size(); i++){
			sizeAllSet.add(info.getByIndex(i).getId());
		}
		
		recurseProjectionSetShrink(sizeAllSet, bestProjectionForSize);
		List<Set<String>> logs = new ArrayList<Set<String>>();
		for(Integer key : bestProjectionForSize.keySet()){
			if(key>2){
				logs.add(bestProjectionForSize.get(key));
			}
		}
		return logs;
	}
	
	public void recurseProjectionSetShrink(Set<String> currentProjection, Map<Integer, Set<String>> bestProjectionForSize){
		if(currentProjection.isEmpty())
			return;
		else{
			hashset.add(currentProjection);
			double minEntropy = Double.MAX_VALUE;
			Set<String> argMinEntropy = null;
			for(String toRemove : currentProjection){
				Set<String> newSet = new HashSet<String>(currentProjection);
				newSet.remove(toRemove);
				XLog tempLog = projectLogOnEventNames(log, newSet);
				double entropy = getEntropyFromDfpg(ConvertLogToDfpg.log2Dfpg(tempLog));
				System.out.println("Entropy: "+entropy+" with set: "+newSet);
				if(entropy<minEntropy){
					minEntropy = entropy;
					argMinEntropy = newSet;
				}
			}
			System.err.println("Entropy: "+minEntropy+" with set: "+argMinEntropy);

			System.out.println();
			bestProjectionForSize.put(argMinEntropy.size(), argMinEntropy);
			recurseProjectionSetShrink(argMinEntropy, bestProjectionForSize);
		}
	}
	
	private static double getEntropyFromDfpg(Dfpg dfpg) {
		Graph<XEventClass> dfg = dfpg.getDirectlyFollowsGraph();
		double result = 0d;
		for(XEventClass xec : dfpg.getActivities()){
			// Calculate original distributions for xec
			long total = 0;
			List<Long> edgeWeightList = new LinkedList<Long>();
			for(Long edgeId : dfg.getOutgoingEdgesOf(xec)){
				long w = dfg.getEdgeWeight(edgeId);
				if(w>0){
					total += w;
					edgeWeightList.add(w);
				}
			}
			Long endCount = dfpg.getEndActivities().getCardinalityOf(xec);
			if(endCount>0){
				total += endCount;
				edgeWeightList.add(endCount);
			}

			double[] distribution = new double[edgeWeightList.size()];
			for(int i=0; i<edgeWeightList.size(); i++)
				distribution[i] = ((double)edgeWeightList.get(i))/total;
			double originalEntropy = getEntropyFromDistribution(distribution);
			if(Double.isNaN(originalEntropy))
				originalEntropy = 0;
			result += originalEntropy;
		}
		
		Graph<XEventClass> dpg = dfpg.getDirectlyPrecedesGraph();
		for(XEventClass xec : dfpg.getActivities()){
			// Calculate original distributions for xec
			long total = 0;
			List<Long> edgeWeightList = new LinkedList<Long>();
			for(Long edgeId : dpg.getOutgoingEdgesOf(xec)){
				long w = dpg.getEdgeWeight(edgeId);
				if(w>0){
					total += w;
					edgeWeightList.add(w);
				}
			}
			Long startCount = dfpg.getStartActivities().getCardinalityOf(xec);
			if(startCount>0){
				total += startCount;
				edgeWeightList.add(startCount);
			}

			double[] distribution = new double[edgeWeightList.size()];
			for(int i=0; i<edgeWeightList.size(); i++){
				distribution[i] = ((double)edgeWeightList.get(i))/total;
			}
			double originalEntropy = getEntropyFromDistribution(distribution);
			
			if(Double.isNaN(originalEntropy))
				originalEntropy = 0;
			result += originalEntropy;
		}
		return result;
	}
	
	public static double getEntropyFromDistribution(double[] distribution){
		/*
		double sum = 0d;
		for(double d : distribution)
			sum+=d;
		
		if(sum!=1)
			System.err.println("distribution sums to "+sum+"!");
		*/
		double entropy = 0d;
		for(double d : distribution)
			if(d>0)
				entropy += -d*DoubleMath.log2(d);
		return entropy;
	}
	
	public static String printDoubleArray(double[] array){
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		boolean nonfirst = false;
		for(double a : array){
			if(nonfirst)
				sb.append(", ");
			sb.append(a);
			nonfirst = true;
		}
		sb.append(']');
		return sb.toString();
	}
}