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
		name = "Filter Log using Entropy (Direct)", 
		parameterLabels = {"Input Log"}, 
	    returnLabels = {"Logs"}, 
	    returnTypes = { List.class }
		)
public class DirectEntropyBasedLogFilter{
	private XLog log;
	private Map<Integer, Set<String>> bestProjectionForSize;
	
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
	@PluginVariant(variantLabel = "Filter Log using Entropy (Direct)", requiredParameterLabels = {0})
	public List<Set<String>> getProjections(PluginContext context, XLog log) {
		this.log = log;
		bestProjectionForSize = new HashMap<Integer, Set<String>>();
		
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
			double maxEntropy = 0d;
			Set<String> argMaxEntropy = null;
			XLog tempLog = projectLogOnEventNames(log, currentProjection);
			for(String toRemove : currentProjection){
				Set<String> newSet = new HashSet<String>(currentProjection);
				newSet.remove(toRemove);
				double entropy = getEntropyFromDfpg(ConvertLogToDfpg.log2Dfpg(tempLog), toRemove);
				System.out.println("Entropy: "+entropy+" with set: "+newSet);
				if(entropy>maxEntropy){
					maxEntropy = entropy;
					argMaxEntropy = newSet;
				}
			}
			System.err.println("Entropy: "+maxEntropy+" with set: "+argMaxEntropy);

			System.out.println();
			bestProjectionForSize.put(argMaxEntropy.size(), argMaxEntropy);
			recurseProjectionSetShrink(argMaxEntropy, bestProjectionForSize);
		}
	}
	
	private static double getEntropyFromDfpg(Dfpg dfpg, String toRemove) {
		Graph<XEventClass> dfg = dfpg.getDirectlyFollowsGraph();
		double result = 0d;
		double d = dfpg.getActivitiesCounts().toSet().size()+1;
		
		for(XEventClass xec : dfpg.getActivities()){
			if(!xec.getId().equals(toRemove))
				continue;
			// Calculate original distributions for xec
			long total = 0;
			List<Long> edgeWeightList = new LinkedList<Long>();
			for(XEventClass xec2 : dfpg.getActivities()){
				long w = dfg.getEdgeWeight(xec, xec2);
				if(w>0){
					total += w;
				}
				edgeWeightList.add(w);
			}
			Long endCount = dfpg.getEndActivities().getCardinalityOf(xec);
			if(endCount>0){
				total += endCount;
			}
			edgeWeightList.add(endCount);

			double[] distribution = new double[edgeWeightList.size()];
			for(int i=0; i<edgeWeightList.size(); i++)
				distribution[i] = ((double)edgeWeightList.get(i)+(1d/d))/(total+1);
			double originalEntropy = getEntropyFromDistribution(distribution);
			if(Double.isNaN(originalEntropy))
				originalEntropy = 0;
			result += originalEntropy;
		}
		
		Graph<XEventClass> dpg = dfpg.getDirectlyPrecedesGraph();
		for(XEventClass xec : dfpg.getActivities()){
			if(!xec.getId().equals(toRemove))
				continue;
			
			// Calculate original distributions for xec
			long total = 0;
			List<Long> edgeWeightList = new LinkedList<Long>();
			for(XEventClass xec2 : dfpg.getActivities()){
				long w = dpg.getEdgeWeight(xec, xec2);
				if(w>0){
					total += w;
				}
				edgeWeightList.add(w);
			}
			Long startCount = dfpg.getStartActivities().getCardinalityOf(xec);
			if(startCount>0){
				total += startCount;
			}
			edgeWeightList.add(startCount);

			double[] distribution = new double[edgeWeightList.size()];
			for(int i=0; i<edgeWeightList.size(); i++){
				distribution[i] = ((double)edgeWeightList.get(i)+(1d/d))/(total+1);
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