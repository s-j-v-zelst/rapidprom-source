package org.rapidprom.operators.niek;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.SetStringIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class ApplyProjectionOperator extends Operator{
	private InputPort inputXLog = getInputPorts()
			.createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputProjection = getInputPorts()
			.createPort("projection set", SetStringIOObject.class);
	private OutputPort outputEventLog = getOutputPorts()
			.createPort("event log (ProM Event Log)");
	private OutputPort outputExampleSet = getOutputPorts()
			.createPort("example set");
	
	private double coverage_activities = 0d;
	private double coverage_events = 0d;
	
	private final String NAMECOL = "Name";
	private final String VALUECOL = "Value";

	public ApplyProjectionOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog,
				IOObjectCollection.class));
		getTransformer().addRule(new GenerateNewMDRule(outputExampleSet,
				ExampleSet.class));
	}
	
	public XLog projectLogOnEventNames(final XLog log, final Set<String> eventSet){
		XLog logClone = (XLog) log.clone();
		Set<XTrace> traceRemoveSet = new HashSet<XTrace>();
		Set<String> conceptNames = new HashSet<String>();
		long totalEvents = 0;
		long newEvents = 0;
		for(XTrace trace : logClone){
			totalEvents += trace.size();
			newEvents += trace.size();
			Set<XEvent> eventRemoveSet = new HashSet<XEvent>();
			for(XEvent event : trace){
				String cname = event.getAttributes().get("concept:name").toString();
				conceptNames.add(cname);
				if(!eventSet.contains(cname))
					eventRemoveSet.add(event);						
			}
			for(XEvent removeEvent : eventRemoveSet){
				trace.remove(removeEvent);
				
			}
			newEvents -= eventRemoveSet.size();
			if(trace.size()==0)
				traceRemoveSet.add(trace);
		}
		for(XTrace removeTrace : traceRemoveSet)
			logClone.remove(removeTrace);
		coverage_events =  ((double)newEvents)/totalEvents;
		coverage_activities = ((double)eventSet.size())/conceptNames.size();
		return logClone;
	}
	
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: Apply projection");
		long time = System.currentTimeMillis();

		XLogIOObject logWrapper = inputXLog.getData(XLogIOObject.class);
		SetStringIOObject setStringWrapper = inputProjection.getData(SetStringIOObject.class);
		
		XLog log = logWrapper.getArtifact();
		Set<String> projectionSet = setStringWrapper.getArtifact();
		
		XLog projectedLog = projectLogOnEventNames(log, projectionSet);

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		outputEventLog.deliver(new XLogIOObject(projectedLog, pluginContext));
		
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute(this.NAMECOL, Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute(this.VALUECOL, Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		fillTableWithRow(table, "Coverage_activities", coverage_activities, attributes);
		fillTableWithRow(table, "Coverage_events", coverage_events, attributes);

		es = table.createExampleSet();
		outputExampleSet.deliver(es);

		logger.log(Level.INFO, "End: Apply projection" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	private void fillTableWithRow(MemoryExampleTable table, String name,
			Object value, List<Attribute> attributes) {
		// fill table
		DataRowFactory factory = new DataRowFactory(
				DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		Object[] vals = new Object[2];
		vals[0] = name;
		vals[1] = value;
		// convert the list to array
		Attribute[] attribArray = new Attribute[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			attribArray[i] = attributes.get(i);
		}
		DataRow dataRow = factory.create(vals, attribArray);
		table.addDataRow(dataRow);
	}
}
