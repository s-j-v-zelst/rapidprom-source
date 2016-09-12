package org.rapidprom.operators.niek;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XTraceImpl;
import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.tools.LogService;

public class OnlyContainCompleteEventsOperator extends Operator {

	private InputPort inputXLog = getInputPorts()
			.createPort("event log (ProM Event Log)", XLogIOObject.class);
	private OutputPort outputEventLog = getOutputPorts()
			.createPort("event log (ProM Event Log)");

	public OnlyContainCompleteEventsOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog,
				XLogIOObject.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: Only contain lifecycle complete events");
		long time = System.currentTimeMillis();

		XLogIOObject logWrapper = inputXLog.getData(XLogIOObject.class);

		XLog log = logWrapper.getArtifact();
		XLog clone = (XLog) log.clone();
		for(XTrace trace : clone){
			List<XEvent> newTrace = new LinkedList<XEvent>();
			if(trace==null)
				continue;
			for(XEvent event : trace){
				if(event==null)
					continue;
				XAttributeMap xam = event.getAttributes();
				if(xam==null || !xam.containsKey("lifecycle:transition"))
					continue;
				
				if(event.getAttributes().get("lifecycle:transition").toString().equals("complete"))
					newTrace.add(event);
			}
			trace = new XTraceImpl(trace.getAttributes());
			for(XEvent newEvent : newTrace)
				trace.add(newEvent);
		}
		
		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		XLogIOObject result = new XLogIOObject(clone, pluginContext);
		
		outputEventLog.deliver(result);

		logger.log(Level.INFO, "End: Only contain lifecycle complete events ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

}