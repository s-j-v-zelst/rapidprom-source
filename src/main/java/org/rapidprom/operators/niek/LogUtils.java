package org.rapidprom.operators.niek;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.log.utils.XLogBuilder;
import org.processmining.xeslite.lite.factory.XFactoryLiteImpl;

public final class LogUtils {
	private LogUtils(){};
	
	public static XLog filterEmptyTraces(final XLog log){
		Set<Integer> removeSet = new HashSet<Integer>();
		for(int i=0;i<log.size(); i++){
			XTrace trace = log.get(i);
			if(trace.size()<1)
				removeSet.add(i);
		}
		int removed = 0;
		for(int i : removeSet){
			log.remove(i-removed);
			removed++;
		}
		return log;
	}
	
	public static XLog logDiff(final XLog log1, final XLog log2){
		XLog log1Clone = (XLog) log1.clone();
		XEventNameClassifier xec = new XEventNameClassifier();
		Set<XTrace> removeSet = new HashSet<XTrace>();
		for(XTrace trace1 : log1Clone){
			boolean presentInLog2 = false;
			for(XTrace trace2 : log2){
				boolean traceEqual = false;
				if(trace1.size()==trace2.size()){
					traceEqual = true;
					for(int i=0; i<trace1.size(); i++){
						if(!xec.sameEventClass(trace1.get(i), trace2.get(i))){
							traceEqual = false;
							break;
						}
					}
				}
				if(traceEqual){
					presentInLog2 = true;
					break;
				}
			}
			if(presentInLog2){
				removeSet.add(trace1);
			}
		}
		for(XTrace trace : removeSet)
			log1Clone.remove(trace);
		return log1Clone;
	}
	
	public static int getMostFrequentActivityCount(final XLog log){
		Map<String, Integer> retVal = getActivityCounts(log);
		int maximum = 0;
		for(String key : retVal.keySet()){
			int c = retVal.get(key);
			if(c>maximum)
				maximum = c;
		}
		return maximum;
	}
	
	public static Map<String, Integer> getActivityCounts(final XLog log){
		Map<String, Integer> retVal = new HashMap<String, Integer>();
		for(XTrace trace : log){
			for(XEvent event : trace){
				String label = event.getAttributes().get("concept:name").toString();
				Integer current = retVal.get(label);
				if(current==null)
					current=0;
				current++;
				retVal.put(label, current);
			}
		}
		return retVal;
	}
	
	public static XLog environmentProjectLogOnEventNames(final XLog log, final Set<String> eventSet){
		XLog logClone = (XLog) log.clone();
		for(XTrace trace : logClone){
			for(XEvent event : trace){
				if(!eventSet.contains(event.getAttributes().get("concept:name").toString()))
					event.getAttributes().put("concept:name", new XAttributeLiteralImpl("concept:name", "environment"));					
			}
		}
		return logClone;
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
	
	public static XLog generateLogFromAttributePerspective(final XLog log, final String attributeName){
		Map<String, Set<XEvent>> resouceToEventMap = new HashMap<String, Set<XEvent>>();
		XLog clone = (XLog) log.clone();
		for(XTrace trace : clone){
			for(XEvent event : trace){
				XAttributeMap map = event.getAttributes();
				if(!map.containsKey(attributeName))
					continue;
				String resource = map.get(attributeName).toString();
				Set<XEvent> eventSet = resouceToEventMap.get(resource);
				if(eventSet==null)
					eventSet = new HashSet<XEvent>();
				map.put("case_id", new XAttributeLiteralImpl("case_id", trace.getAttributes().get("concept:name").toString()));
				eventSet.add(event);
				resouceToEventMap.put(resource, eventSet);
			}
		}
		XLogBuilder xlb = XLogBuilder.newInstance();
		xlb.startLog(attributeName);
		for(String resource : resouceToEventMap.keySet()){
			Set<XEvent> eventSet = resouceToEventMap.get(resource);
			xlb.addTrace(resource);
			for(XEvent event : eventSet){
				XAttributeMap xam = event.getAttributes();
				if(!xam.containsKey("concept:name"))
					continue;
				xlb.addEvent(xam.get("concept:name").toString());
				for(XAttribute xa : xam.values()){
					if(xa instanceof XAttributeBoolean){
						xlb.addAttribute(xa.getKey(), ((XAttributeBoolean) xa).getValue());
					}
					if(xa instanceof XAttributeContinuous){
						xlb.addAttribute(xa.getKey(), ((XAttributeContinuous) xa).getValue());
					}
					if(xa instanceof XAttributeDiscrete){
						xlb.addAttribute(xa.getKey(), ((XAttributeDiscrete) xa).getValue());
					}
					if(xa instanceof XAttributeLiteral){
						xlb.addAttribute(xa.getKey(), ((XAttributeLiteral) xa).getValue());
					}
					if(xa instanceof XAttributeTimestamp){
						xlb.addAttribute(xa.getKey(), ((XAttributeTimestamp) xa).getValue());
					}
				}
			}
		}
		return xlb.build();
	}
	
	public static Map<String,Character> getLogEncodingScheme(final XLog log){
		Map<String, Character> eventEncoding = new HashMap<String, Character>();
		char currentChar = '0';
		for(XTrace trace : log){
			for(XEvent event : trace){
				String conceptName = event.getAttributes().get("concept:name").toString();
				if(!eventEncoding.containsKey(conceptName)){
					eventEncoding.put(conceptName, currentChar);
					currentChar++;
				}
			}
		}
		return eventEncoding;
	}
	
	public static Map<Character,String> getLogDecodingScheme(final Map<String, Character> eventEncoding){
		Map<Character,String> eventDecoding = new HashMap<Character,String>();
		for(String key : eventEncoding.keySet())
			eventDecoding.put(eventEncoding.get(key), key);
		return eventDecoding;
	}
	
	public static Map<Character,String> getLogDecodingScheme(final XLog log){
		Map<String, Character> eventEncoding = getLogEncodingScheme(log);
		return getLogDecodingScheme(eventEncoding);
	}
	
	public static XLog encodeLog(final XLog log) {
		Map<String, Character> eventEncoding = new HashMap<String, Character>();
		char currentChar = '0';
		XLogBuilder xlb = XLogBuilder.newInstance();
		//xlb.setFactory(new XFactoryLiteImpl()); TODO: import newer version of RapidProM and enable this again
		xlb.startLog("Encoded Log");
		for(XTrace trace : log){
			xlb.addTrace(trace.getAttributes().get("concept:name").toString());
			for(XEvent event : trace){
				String conceptName = event.getAttributes().get("concept:name").toString();
				if(!eventEncoding.containsKey(conceptName)){
					eventEncoding.put(conceptName, currentChar);
					currentChar++;
				}
				xlb.addEvent(""+eventEncoding.get(conceptName));
			}
		}
	    return xlb.build();
	}
	
	public static XLog encodeLogUsingScheme(final XLog log, final Map<String, Character> eventEncoding) {
		XLogBuilder xlb = XLogBuilder.newInstance();
		//xlb.setFactory(new XFactoryLiteImpl());
		xlb.startLog("Encoded Log");
		for(XTrace trace : log){
			xlb.addTrace(trace.getAttributes().get("concept:name").toString());
			for(XEvent event : trace){
				String conceptName = event.getAttributes().get("concept:name").toString();
				xlb.addEvent(""+eventEncoding.get(conceptName));
			}
		}
	    return xlb.build();
	}
}