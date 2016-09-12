package org.rapidprom.operators.niek;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginVariant;


@Plugin(
		name = "Filter Log using Frequency (least frequent first)", 
		parameterLabels = {"Input Log"}, 
	    returnLabels = {"Logs"}, 
	    returnTypes = { List.class }
		)
public class FrequencyBasedLogFilter1{

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
	@PluginVariant(variantLabel = "Filter Log using Frequency (least frequent first)", requiredParameterLabels = {0})
	public List<XLog> getProjections(PluginContext context, XLog log) {
		List<XLog> logs = new ArrayList<XLog>();

		// build initial set
		XLogInfo info = XLogInfoFactory.createLogInfo(log, new XEventNameClassifier());
		XEventClasses activities = info.getEventClasses();
		List<XEventClass> sortedActivities = sortEvents(activities);
		Set<XEventClass> currentActivities = new HashSet<XEventClass>(sortedActivities);
		int size = activities.size();
		for(XEventClass activityToRemove : sortedActivities){
			currentActivities.remove(activityToRemove);
			if(currentActivities.size()<=2)
				break;
			size--;
			XLog tempLog = projectLogOnEventNames(log, getNames(currentActivities));
			tempLog.getAttributes().put("concept:name", new XAttributeLiteralImpl("concept:name", "size "+size));
			logs.add(tempLog);
		}
		Collections.reverse(logs);
		return logs;
	}
	
	private Set<String> getNames(Collection<XEventClass> activities){
		Set<String> names = new HashSet<String>();
		for(XEventClass activity : activities)
			names.add(activity.getId());
		return names;
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
				if (current.size() > event.size()) {
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