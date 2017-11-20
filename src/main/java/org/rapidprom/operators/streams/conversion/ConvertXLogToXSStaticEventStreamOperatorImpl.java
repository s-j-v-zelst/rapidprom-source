package org.rapidprom.operators.streams.conversion;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.eventstream.algorithms.XLogToXSStaticXSEventStreamAlgorithm;
import org.processmining.eventstream.core.interfaces.XSStaticXSEventStream;
import org.processmining.eventstream.parameters.XLogToXSStaticXSEventStreamParameters;
import org.processmining.eventstream.parameters.XLogToXSStaticXSEventStreamParameters.EmissionOrdering;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.streams.event.XSStaticXSEventStreamIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.tools.LogService;

public class ConvertXLogToXSStaticEventStreamOperatorImpl extends AbstractRapidProMEventLogBasedOperator {

	private OutputPort outputPortstaticStream = getOutputPorts().createPort("static event stream");

	private final String PARAMETER_KEY_EMISSION_ORDER = "Emission ordering";
	private final String PARAMETER_DESC_EMISSION_ORDER = "Determines the ordering of the events in the stream";
	private final String[] PARAMETER_VALUE_EMISSION_ORDER = new String[] {
			XLogToXSStaticXSEventStreamParameters.EmissionOrdering.TIME_STAMP.toString(),
			XLogToXSStaticXSEventStreamParameters.EmissionOrdering.LOG.toString() };

	private final String PARAMETER_KEY_ADDITIONAL_DECORATION = "Add all event attributes";
	private final String PARAMETER_DESC_ADDITIONAL_DECORATION = "If checked, all event attributes are copied into the streamed events.";

	public ConvertXLogToXSStaticEventStreamOperatorImpl(OperatorDescription description) {
		super(description);
		getTransformer().addGenerationRule(outputPortstaticStream, XSStaticXSEventStreamIOObject.class);
	}

	@Override
	public void doWork() throws UserError {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "start do work static stream generator (xlog)");
		XLogToXSStaticXSEventStreamParameters algorithmParameters = new XLogToXSStaticXSEventStreamParameters();
		if (getParameterAsString(PARAMETER_KEY_EMISSION_ORDER).equals(EmissionOrdering.TIME_STAMP.toString())) {
			algorithmParameters.setEmissionOrdering(EmissionOrdering.TIME_STAMP);
		} else if (getParameterAsString(PARAMETER_KEY_EMISSION_ORDER).equals(EmissionOrdering.LOG.toString())) {
			algorithmParameters.setEmissionOrdering(EmissionOrdering.LOG);
		}
		algorithmParameters.setAdditionalDecoration(getParameterAsBoolean(PARAMETER_KEY_ADDITIONAL_DECORATION));
		algorithmParameters.setEventClassifier(getXEventClassifier());
		algorithmParameters.setTraceClassifier(
				new XAttributeLiteralImpl(XConceptExtension.KEY_NAME, "", XConceptExtension.instance()));
		XLogToXSStaticXSEventStreamAlgorithm algorithm;
		XSStaticXSEventStream staticStream;
		try {
			algorithm = new XLogToXSStaticXSEventStreamAlgorithm(RapidProMGlobalContext.instance().getPluginContext(),
					getXLog(), algorithmParameters);
			staticStream = algorithm.get();
		} catch (Exception e) {
			algorithmParameters.setEmissionOrdering(EmissionOrdering.LOG);
			algorithm = new XLogToXSStaticXSEventStreamAlgorithm(RapidProMGlobalContext.instance().getPluginContext(),
					getXLog(), algorithmParameters);
			staticStream = algorithm.get();
		}
		outputPortstaticStream.deliver(
				new XSStaticXSEventStreamIOObject(staticStream, RapidProMGlobalContext.instance().getPluginContext()));
		logger.log(Level.INFO, "end do work static stream generator (xlog)");
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		ParameterTypeCategory emissionOrdering = new ParameterTypeCategory(PARAMETER_KEY_EMISSION_ORDER,
				PARAMETER_DESC_EMISSION_ORDER, PARAMETER_VALUE_EMISSION_ORDER, 0, false);
		params.add(emissionOrdering);

		ParameterTypeBoolean addData = new ParameterTypeBoolean(PARAMETER_KEY_ADDITIONAL_DECORATION,
				PARAMETER_DESC_ADDITIONAL_DECORATION, true, false);
		params.add(addData);

		return params;

	}

}
