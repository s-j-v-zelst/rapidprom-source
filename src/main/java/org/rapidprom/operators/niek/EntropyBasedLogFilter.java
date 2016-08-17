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
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginVariant;
import org.processmining.plugins.InductiveMiner.graphs.Graph;

import com.google.common.math.DoubleMath;


@Plugin(
		name = "Filter Log using Entropy (Global)", 
		parameterLabels = {"Input Log"}, 
	    returnLabels = {"Logs"}, 
	    returnTypes = { List.class }
		)
public class EntropyBasedLogFilter{
	private XLog log;
	private Map<Integer, Set<String>> bestProjectionForSize;
	private Map<Integer, Double> bestEntropyPerLength;
	private Set<Set<String>> hashset;
	
	
	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "N. Tax", email = "n.tax@tue.nl")
	@PluginVariant(variantLabel = "Filter Log using Entropy (Global)", requiredParameterLabels = {0})
	public List<XLog> getProjections(PluginContext context, XLog log) {
		this.log = log;
		bestProjectionForSize = new HashMap<Integer, Set<String>>();
		bestEntropyPerLength = new HashMap<Integer, Double>();
		hashset = new HashSet<Set<String>>();
		
		// build initial set
		XEventClasses info = XLogInfoFactory.createLogInfo(log, new XEventNameClassifier()).getEventClasses();
		Set<String> sizeAllSet = new HashSet<String>();
		for(int i=0; i<info.size(); i++){
			sizeAllSet.add(info.getByIndex(i).getId());
		}

		List<XLog> logs = new ArrayList<XLog>();

		Set<String> emptySet = new HashSet<String>();
		XLog tLog = LogUtils.projectLogOnEventNames(log, emptySet);
		tLog.getAttributes().put("concept:name", new XAttributeLiteralImpl("concept:name", "size 0"));
		logs.add(tLog);
		
		recurseProjectionSetShrink(sizeAllSet, bestProjectionForSize, bestEntropyPerLength);
		for(Integer key : bestProjectionForSize.keySet()){
			if(key<sizeAllSet.size()){
				XLog tempLog = LogUtils.projectLogOnEventNames(log, bestProjectionForSize.get(key));
				tempLog.getAttributes().put("concept:name", new XAttributeLiteralImpl("concept:name", "size "+key));
				logs.add(tempLog);
			}
		}
		return logs;
	}
	
	public void recurseProjectionSetShrink(Set<String> currentProjection, Map<Integer, Set<String>> bestProjectionForSize, Map<Integer, Double> performanceSet){
		if(currentProjection.isEmpty() || hashset.contains(currentProjection))
			return;
		else{
			hashset.add(currentProjection);
			XLog tempLog = LogUtils.projectLogOnEventNames(log, currentProjection);
			double entropy = getEntropyFromDfpg(ConvertLogToDfpg.log2Dfpg(tempLog));
			if(!performanceSet.containsKey(currentProjection.size()) || (entropy<performanceSet.get(currentProjection.size()))){
				performanceSet.put(currentProjection.size(), entropy);
				bestProjectionForSize.put(currentProjection.size(), currentProjection);
			}
		}
		for(String toRemove : currentProjection){
			Set<String> newSet = new HashSet<String>(currentProjection);
			newSet.remove(toRemove);
			recurseProjectionSetShrink(newSet, bestProjectionForSize, performanceSet);
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