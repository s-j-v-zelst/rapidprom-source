package org.rapidprom.operators.niek;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.deckfour.xes.model.impl.XEventImpl;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginVariant;

@Plugin(
		name = "Add Noisy Activities to Log", 
		parameterLabels = {"Input Log"}, 
	    returnLabels = {"Log with noisy activities"}, 
	    returnTypes = { XLog.class }
		)
public class AddNoisyActivitiesToLog {
	private Random random = new Random();

	
	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "N. Tax", email = "n.tax@tue.nl")
	@PluginVariant(variantLabel = "Add Noisy Activities to Log", requiredParameterLabels = {0})
	public XLog transform(PluginContext context, XLog log) {
		return transform(context, log, 5);
	}
	
	public XLog transform(PluginContext context, XLog log, int numberOfNoisyActivities) {
		// build initial set
		XLog clone = (XLog) log.clone();
		XLogInfo info = XLogInfoFactory.createLogInfo(clone, new XEventNameClassifier());
		XEventClasses activities = info.getEventClasses();
		List<XEventClass> sortedActivities = sortEvents(activities);
		int noisyRemaining = numberOfNoisyActivities;
		int normalRemaining = sortedActivities.size();
		List<Integer> randomFrequencies = new LinkedList<Integer>();
		int previousNormalFrequency = sortedActivities.get(0).size();
		int nextNormalFrequency = sortedActivities.get(0).size();
		for(int i=0; i<(numberOfNoisyActivities+sortedActivities.size()); i++){
			float probability = ((float)noisyRemaining)/(noisyRemaining+normalRemaining);
			if(getRandomBoolean(probability)){
				// add a random noisy activity
				randomFrequencies.add((previousNormalFrequency+nextNormalFrequency)/2);
				noisyRemaining--;
			}else{
				// add the next normal activity
				normalRemaining--;
				previousNormalFrequency = sortedActivities.get(sortedActivities.size()-normalRemaining-1).size();
				if(normalRemaining>0)
					nextNormalFrequency = sortedActivities.get(sortedActivities.size()-normalRemaining).size();
				else
					previousNormalFrequency = sortedActivities.get(sortedActivities.size()-normalRemaining-1).size();
			}
		}

		for(int activity=1; activity<=randomFrequencies.size(); activity++){
			int count = randomFrequencies.get(activity-1);
			for(int i=0; i<count; i++){
				int location = random.nextInt(getNumberOfEvents(clone)+clone.size());
				int pos = 0;
				for(XTrace trace : clone){
					if(location>=pos && location<=(pos+trace.size())){// insert in this trace
						XEvent event = new XEventImpl();
						event.getAttributes().put("concept:name", new XAttributeLiteralImpl("concept:name", ""+activity));
						event.getAttributes().put("lifecycle:transition", new XAttributeLiteralImpl("lifecycle:transition", "complete"));
						trace.add(location-pos, event);
					}
					pos+=trace.size()+1;
				}
			}
		}
			
		return clone;
	}
	
	public int getNumberOfEvents(XLog log){
		int numEvents = 0;
		for(XTrace trace : log)
			numEvents+=trace.size();
		return numEvents;
	}
	
	public boolean getRandomBoolean(float p){
		return random.nextFloat() < p;
	}
	
	/**
	 * Sorts the given event classes from low to high occurrence.
	 * 
	 * @param eventClasses
	 *            The given event classes.
	 * @return The sorted event classes.
	 */
	private ArrayList<XEventClass> sortEvents(XEventClasses eventClasses) {
		ArrayList<XEventClass> sortedEvents = new ArrayList<XEventClass>();
		for (XEventClass event : eventClasses.getClasses()) {
			boolean inserted = false;
			XEventClass current = null;
			for (int i = 0; i < sortedEvents.size(); i++) {
				current = sortedEvents.get(i);
				if (current.size() < event.size()) {
					// insert at correct position and set marker
					sortedEvents.add(i, event);
					inserted = true;
					break;
				}
			}
			if (inserted == false) {
				// append to end of list
				sortedEvents.add(event);
			}
		}
		return sortedEvents;
	}
}
