package org.rapidprom.operators.logmanipulation;

import java.util.Iterator;
import java.util.Set;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.rapidprom.ioobjects.TransEvMappingIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;

public class FilterUnamppedEventsOperator extends Operator {

	private InputPort inputLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputTransitionMapping = getInputPorts()
			.createPort("mapping (ProM Transition/Event Class Mapping)", TransEvMappingIOObject.class);

	private OutputPort outputLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public FilterUnamppedEventsOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		XLogIOObject logIO = inputLog.getData(XLogIOObject.class);
		MetaData md = inputLog.getMetaData();
		XLog log = logIO.getArtifact();
		TransEvClassMapping mapping = inputTransitionMapping.getData(TransEvMappingIOObject.class).getArtifact();
		XLog filteredLog = filter(mapping, log);
		XLogIOObject xLogIOObject = new XLogIOObject(filteredLog, logIO.getPluginContext());
		outputLog.deliverMD(md);
		outputLog.deliver(xLogIOObject);
	}
	
	//TODO replace by LogEnhancement version after update
	public XLog filter(TransEvClassMapping mapping, XLog log) {

		Set<String> mappedClassIdentifies = ImmutableSet
				.copyOf(Sets.newHashSet(Collections2.transform(mapping.values(), new Function<XEventClass, String>() {

					public String apply(XEventClass eventClass) {
						return eventClass.getId();
					}
				})));
		XEventClassifier classifier = mapping.getEventClassifier();

		XLog newLog = (XLog) log.clone();

		for (XTrace t : newLog) {
			for (Iterator<XEvent> iterator = t.iterator(); iterator.hasNext();) {
				XEvent event = iterator.next();
				if (!mappedClassIdentifies.contains(classifier.getClassIdentity(event))) {
					iterator.remove();
				}
			}
		}

		return newLog;
	}

}