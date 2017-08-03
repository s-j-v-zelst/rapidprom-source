package org.rapidprom.operators.logmanipulation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.tools.LogService;

//creates a prefix closure, appends "_prefix_
public class PrefixClosureOperatorImpl extends Operator {

	private InputPort inputLog = getInputPorts().createPort("event log", XLogIOObject.class);
	private OutputPort outputLog = getOutputPorts().createPort("event log ");

	public static final String PREFIX_NAME_SUFFIX = "_prefix_";

	public PrefixClosureOperatorImpl(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputLog, XLogIOObject.class));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "start: create prefix-closure");
		long time = System.currentTimeMillis();

		MetaData logMd = inputLog.getMetaData();

		XLog original = inputLog.getData(XLogIOObject.class).getArtifact();
		XLog clone = (XLog) original.clone();

		XFactory fct = XFactoryRegistry.instance().currentDefault();

		Collection<XTrace> prefixes = new HashSet<>();
		for (XTrace trace : clone) {
			String traceName = trace.getAttributes().get(XConceptExtension.KEY_NAME).toString();
			for (int i = 1; i < trace.size(); i++) {
				List<XEvent> prefix = trace.subList(0, i);
				List<XEvent> clonedPrefix = new ArrayList<>();
				for (XEvent e : prefix) {
					clonedPrefix.add((XEvent) e.clone());
				}
				XAttributeMap map = fct.createAttributeMap();
				map.putAll((Map<? extends String, ? extends XAttribute>) trace.getAttributes().clone());
				map.put(XConceptExtension.KEY_NAME,
						new XAttributeLiteralImpl(XConceptExtension.KEY_NAME, traceName + PREFIX_NAME_SUFFIX + i));
				XTrace prefixObj = fct.createTrace(map);
				prefixObj.addAll(clonedPrefix);
				prefixes.add(prefixObj);
			}
		}
		clone.addAll(prefixes);

		outputLog.deliverMD(logMd);
		outputLog.deliver(new XLogIOObject(clone, RapidProMGlobalContext.instance().getPluginContext()));

		logger.log(Level.INFO, "end: create prefix-closure (" + (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

}
