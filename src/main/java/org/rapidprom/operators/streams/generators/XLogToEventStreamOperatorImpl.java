package org.rapidprom.operators.streams.generators;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.eventstream.algorithms.XLogToXSStaticXSEventStreamAlgorithm;
import org.processmining.eventstream.authors.staticeventstream.plugins.XSStaticXSEventStreamToXSEventStreamPlugin;
import org.processmining.eventstream.core.interfaces.XSEvent;
import org.processmining.eventstream.core.interfaces.XSEventStream;
import org.processmining.eventstream.core.interfaces.XSStaticXSEventStream;
import org.processmining.eventstream.parameters.XLogToXSStaticXSEventStreamParameters;
import org.processmining.eventstream.parameters.XLogToXSStaticXSEventStreamParameters.EmissionOrdering;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.stream.core.interfaces.XSAuthor;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.streams.XSAuthorIOObject;
import org.rapidprom.ioobjects.streams.event.XSEventStreamIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

/**
 * integrates conversion to static stream and subsequent conversion to active
 * stream
 * 
 * @author svzelst
 *
 */
public class XLogToEventStreamOperatorImpl extends AbstractRapidProMEventLogBasedOperator {

	private OutputPort outputAuthor = getOutputPorts().createPort("generator");
	private OutputPort outputStream = getOutputPorts().createPort("stream");
	private OutputPort outputStatistics = getOutputPorts().createPort("statistics");

	private final String PARAMETER_KEY_EMISSION_ORDER = "Emission ordering";
	private final String PARAMETER_DESC_EMISSION_ORDER = "Determines the ordering of the events in the stream";
	private final String[] PARAMETER_VALUE_EMISSION_ORDER = new String[] {
			XLogToXSStaticXSEventStreamParameters.EmissionOrdering.TIME_STAMP.toString(),
			XLogToXSStaticXSEventStreamParameters.EmissionOrdering.LOG.toString() };

	private final String PARAMETER_KEY_ADDITIONAL_DECORATION = "Add all event attributes";
	private final String PARAMETER_DESC_ADDITIONAL_DECORATION = "If checked, all event attributes are copied into the streamed events.";

	private final String STATISTICS_COLUMN_NAME_NUM_EVENTS = "number_of_events";
	private final int STATISTICS_COLUMN_TYPE_NUM_EVENTS = Ontology.INTEGER;

	public XLogToEventStreamOperatorImpl(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputAuthor, XSAuthorIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputStream, XSEventStreamIOObject.class));

		ExampleSetMetaData md = new ExampleSetMetaData();
		AttributeMetaData amd = new AttributeMetaData(STATISTICS_COLUMN_NAME_NUM_EVENTS,
				STATISTICS_COLUMN_TYPE_NUM_EVENTS);
		amd.setRole(AttributeColumn.REGULAR);
		amd.setNumberOfMissingValues(new MDInteger(0));
		md.addAttribute(amd);
		getTransformer().addRule(new GenerateNewMDRule(outputStatistics, md));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doWork() throws UserError {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "start do work stream generator (xlog)");
		XLogToXSStaticXSEventStreamParameters params = new XLogToXSStaticXSEventStreamParameters();
		XLogToXSStaticXSEventStreamParameters.EmissionOrdering emission = XLogToXSStaticXSEventStreamParameters.EmissionOrdering.TIME_STAMP;
		if (getParameterAsString(PARAMETER_KEY_EMISSION_ORDER).equals(EmissionOrdering.TIME_STAMP)) {
			emission = EmissionOrdering.TIME_STAMP;
		} else if (getParameterAsString(PARAMETER_KEY_EMISSION_ORDER).equals(EmissionOrdering.LOG)) {
			emission = EmissionOrdering.LOG;
		}
		params.setEmissionOrdering(emission);
		params.setAdditionalDecoration(getParameterAsBoolean(PARAMETER_KEY_ADDITIONAL_DECORATION));
		params.setEventClassifier(getXEventClassifier());
		params.setTraceClassifier(
				new XAttributeLiteralImpl(XConceptExtension.KEY_NAME, "", XConceptExtension.instance()));
		XLogToXSStaticXSEventStreamAlgorithm algorithm;
		XSStaticXSEventStream staticStream;
		try { // at the moment we do not check whether the log actually contains
				// timestamps
			algorithm = new XLogToXSStaticXSEventStreamAlgorithm(RapidProMGlobalContext.instance().getPluginContext(),
					getXLog(), params);
			staticStream = algorithm.get();
		} catch (NullPointerException e) {
			params.setEmissionOrdering(EmissionOrdering.LOG);
			algorithm = new XLogToXSStaticXSEventStreamAlgorithm(RapidProMGlobalContext.instance().getPluginContext(),
					getXLog(), params);
			staticStream = algorithm.get();
		}
		PluginContext context = RapidProMGlobalContext.instance()
				.getFutureResultAwarePluginContext(XSStaticXSEventStreamToXSEventStreamPlugin.class);
		Object[] authStream = XSStaticXSEventStreamToXSEventStreamPlugin.apply(context, staticStream);

		outputAuthor.deliver(new XSAuthorIOObject<XSEvent>((XSAuthor<XSEvent>) authStream[0], context));
		outputStream.deliver(new XSEventStreamIOObject((XSEventStream) authStream[1], context));

		Attribute numEvents = AttributeFactory.createAttribute(STATISTICS_COLUMN_NAME_NUM_EVENTS,
				STATISTICS_COLUMN_TYPE_NUM_EVENTS);
		ExampleSetBuilder statisticsBuilder = ExampleSets.from(numEvents);
		Object[] values = new Object[] { staticStream.size() };
		DataRowFactory dataRowFactory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		statisticsBuilder.addDataRow(dataRowFactory.create(values, new Attribute[] { numEvents }));
		outputStatistics.deliver(statisticsBuilder.build());
		logger.log(Level.INFO, "end do work stream generator (xlog)");
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
