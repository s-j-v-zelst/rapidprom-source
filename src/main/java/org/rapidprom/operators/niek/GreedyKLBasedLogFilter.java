package org.rapidprom.operators.niek;

import java.text.DecimalFormat;
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
import org.processmining.plugins.InductiveMiner.MultiSet;
import org.processmining.plugins.InductiveMiner.graphs.Graph;


@Plugin(
		name = "Filter Log using KL-divergence (Greedy)", 
		parameterLabels = {"Input Log"}, 
	    returnLabels = {"Logs"}, 
	    returnTypes = { List.class }
		)
public class GreedyKLBasedLogFilter{
	private XLog log;
	private Map<Integer, Set<String>> bestProjectionForSize;
	private Set<Set<String>> hashset;
	private Map<XEventClass, double[]> pFollowsMap;
	private Map<XEventClass, double[]> pPrecedesMap;
	private List<XEventClass> activities;
	
	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "N. Tax", email = "n.tax@tue.nl")
	@PluginVariant(variantLabel = "Filter Log using KL-divergence (Greedy)", requiredParameterLabels = {0})
	public List<XLog> getProjections(PluginContext context, XLog log) {
		this.log = log;
		bestProjectionForSize = new HashMap<Integer, Set<String>>();
		hashset = new HashSet<Set<String>>();
		activities = new LinkedList<XEventClass>();
		
		// build initial set
		XEventClasses info = XLogInfoFactory.createLogInfo(log, new XEventNameClassifier()).getEventClasses();
		activities = new LinkedList<XEventClass>(info.getClasses());
		Set<String> sizeAllSet = new HashSet<String>();
		for(int i=0; i<info.size(); i++)
			sizeAllSet.add(info.getByIndex(i).getId());
		
		// populate p
		Dfpg dfpg = ConvertLogToDfpg.log2Dfpg(log);
		Graph<XEventClass> dfg = dfpg.getDirectlyFollowsGraph();
		MultiSet<XEventClass> counts = dfpg.getActivitiesCounts();
		pFollowsMap = new HashMap<XEventClass, double[]>();
		for(XEventClass xec : activities){
			double[] p = new double[activities.size()+1];
			for(int i=0; i<activities.size(); i++){
				p[i] = ((double) dfg.getEdgeWeight(xec, activities.get(i)))/counts.getCardinalityOf(xec);
			}
			p = makeRemainderDistribution(p);
			pFollowsMap.put(xec, p);
		}
		Graph<XEventClass> dpg = dfpg.getDirectlyPrecedesGraph();
		pPrecedesMap = new HashMap<XEventClass, double[]>();
		for(XEventClass xec : activities){
			double[] p = new double[activities.size()+1];
			for(int i=0; i<activities.size(); i++){
				p[i] = ((double) dpg.getEdgeWeight(xec, activities.get(i)))/counts.getCardinalityOf(xec);
			}
			p = makeRemainderDistribution(p);
			pPrecedesMap.put(xec, p);
		}
		
		recurseProjectionSetShrink(sizeAllSet, bestProjectionForSize);
		List<XLog> logs = new ArrayList<XLog>();
		for(Integer key : bestProjectionForSize.keySet()){
			if(key>2){
				XLog tempLog = projectLogOnEventNames(log, bestProjectionForSize.get(key));
				tempLog.getAttributes().put("concept:name", new XAttributeLiteralImpl("concept:name", "size "+key));
				logs.add(tempLog);
			}
		}
		
		// manual garbage collection
		bestProjectionForSize = null;
		hashset = null;
		pFollowsMap = null;
		pPrecedesMap = null;
		
		return logs;
	}
	
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
	
	public void recurseProjectionSetShrink(Set<String> currentProjection, Map<Integer, Set<String>> bestProjectionForSize){
		if(currentProjection.isEmpty())
			return;
		else{
			hashset.add(currentProjection);
			double minDivergence = Double.MAX_VALUE;
			Set<String> argMaxDivergence = null;
			for(String toRemove : currentProjection){
				Set<String> newSet = new HashSet<String>(currentProjection);
				newSet.remove(toRemove);
				XLog tempLog = projectLogOnEventNames(log, newSet);
				double divergence = getKLDivergenceFromDfpg(ConvertLogToDfpg.log2Dfpg(tempLog));
				System.out.println("KL-divergence: "+divergence+" with set: "+newSet);
				if(divergence<minDivergence){
					minDivergence = divergence;
					argMaxDivergence = newSet;
				}
			}
			System.err.println("KL-divergence: "+minDivergence+" with set: "+argMaxDivergence);

			System.out.println();
			bestProjectionForSize.put(argMaxDivergence.size(), argMaxDivergence);
			recurseProjectionSetShrink(argMaxDivergence, bestProjectionForSize);
		}
	}
	
	private double getKLDivergenceFromDfpg(Dfpg dfpg) {
		Graph<XEventClass> dfg = dfpg.getDirectlyFollowsGraph();
		MultiSet<XEventClass> counts = dfpg.getActivitiesCounts();
		double totalDivergence = 0d;
		for(XEventClass xec : dfpg.getActivities()){
			double[] pAll = pFollowsMap.get(xec);
			List<XEventClass> pProjected = new LinkedList<XEventClass>();
			int i=-1;
			for(XEventClass targetActivity : activities){
				i++;
				if(dfpg.getActivitiesCounts().contains(targetActivity))
					pProjected.add(targetActivity);
			}
			double[] q = new double[pProjected.size()];
			double[] p = new double[pProjected.size()+1];
			i=0;
			for(XEventClass targetActivity : pProjected){
				q[i] = ((double)dfg.getEdgeWeight(xec, targetActivity))/counts.getCardinalityOf(xec);
				p[i] = pAll[i];
				i++;
			}
			p[pProjected.size()] = pAll[pAll.length-1];
			p = renormalizeToDistribution(p);
			q = makeRemainderDistribution(q);
			double localDivergence = getKLDivergence(p, q);
			if(Double.isInfinite(localDivergence)){
				System.err.println(printDoubleArray(p));
				System.err.println(printDoubleArray(q));
				System.out.println();
			}
			if(Double.isNaN(localDivergence))
				localDivergence = 0;
			totalDivergence += localDivergence;
		}
		
		Graph<XEventClass> dpg = dfpg.getDirectlyPrecedesGraph();
		for(XEventClass xec : dfpg.getActivities()){
			double[] pAll = pPrecedesMap.get(xec);
			List<XEventClass> pProjected = new LinkedList<XEventClass>();
			int i=-1;
			for(XEventClass targetActivity : activities){
				i++;
				if(dfpg.getActivitiesCounts().contains(targetActivity))
					pProjected.add(targetActivity);
			}
			double[] q = new double[pProjected.size()];
			double[] p = new double[pProjected.size()+1];
			i=0;
			for(XEventClass targetActivity : pProjected){
				q[i] = ((double)dpg.getEdgeWeight(xec, targetActivity))/counts.getCardinalityOf(xec);
				p[i] = pAll[i];
				i++;
			}
			p[pProjected.size()] = pAll[pAll.length-1];
			p = renormalizeToDistribution(p);
			q = makeRemainderDistribution(q);
			double localDivergence = getKLDivergence(p, q);
			if(Double.isInfinite(localDivergence)){
				System.err.println(printDoubleArray(p));
				System.err.println(printDoubleArray(q));
				System.out.println();
			}
			if(Double.isNaN(localDivergence))
				localDivergence = 0;
			totalDivergence += localDivergence;
		}
		return totalDivergence;
	}
	
	public static double getKLDivergence(double[] p, double[] q){
		assert p.length==q.length;
		double divergence = 0d;
		for(int i=0; i<p.length; i++)
			if(p[i]>0 && q[i]>0)
				divergence += p[i]*(p[i]/q[i]);
		return divergence;
	}
	
	public static double[] renormalizeToDistribution(double[] distribution){
		double total = 0d;
		for(int i =0; i<distribution.length; i++)
			total+=distribution[i];
		for(int i =0; i<distribution.length; i++)
			distribution[i] /= total;
		return distribution;
	}
	
	public static double[] makeRemainderDistribution(double[] distribution){
		double total = 0d;
		double[] newDistribution = new double[distribution.length+1];
		for(int i=0; i<distribution.length; i++){
			newDistribution[i] = distribution[i];
			total += distribution[i];
		}
		assert((1d-total)>=-1d/1000);
		newDistribution[distribution.length] = 1d-total;
		return newDistribution;
	}
	
	public static String printDoubleArray(double[] array){
		DecimalFormat df = new DecimalFormat("#.00"); 
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		boolean nonfirst = false;
		for(double a : array){
			if(nonfirst)
				sb.append(", ");
			sb.append(df.format(a));
			nonfirst = true;
		}
		sb.append(']');
		return sb.toString();
	}
}