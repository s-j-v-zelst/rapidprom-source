package org.rapidprom.operators.streams.filtering;

import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.eventstream.core.factories.XSEventStreamFactory;
import org.processmining.eventstream.core.interfaces.XSEvent;
import org.processmining.eventstream.core.interfaces.XSEventStream;
import org.processmining.stream.core.enums.CommunicationType;
import org.processmining.stream.core.interfaces.XSAuthor;
import org.processmining.streambasedeventfilter.algorithms.ConditionalProbabilitiesBasedXSEventFilterImpl;
import org.processmining.streambasedeventfilter.parameters.ConditionalProbabilitiesBasedXSEventFilterParametersImpl;
import org.processmining.streambasedeventfilter.parameters.ConditionalProbabilitiesBasedXSEventFilterParametersImpl.Abstraction;
import org.processmining.streambasedeventfilter.parameters.ConditionalProbabilitiesBasedXSEventFilterParametersImpl.AdjustmentMethod;
import org.processmining.streambasedeventfilter.parameters.ConditionalProbabilitiesBasedXSEventFilterParametersImpl.FilteringMethod;
import org.processmining.streambasedeventlog.parameters.StreamBasedEventLogParametersImpl;
import org.processmining.yawl.ext.org.apache.commons.lang.ArrayUtils;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.streams.XSAuthorIOObject;
import org.rapidprom.ioobjects.streams.XSHubIOObject;
import org.rapidprom.ioobjects.streams.event.XSEventStreamIOObject;
import org.rapidprom.operators.streams.generators.XLogToEventStreamOperatorImpl;
import org.rapidprom.util.ObjectUtils;

import com.kenai.jffi.Array;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class TrieBasedSpuriousEventFilterOperatorImpl extends Operator {

	private InputPort inputEventStream = getInputPorts().createPort("event stream", XSEventStreamIOObject.class);

	private InputPort inputAuthor = getInputPorts().createPort("author");
	private InputPort inputStreamStatistics = getInputPorts().createPort("statistics");

	private OutputPort outputStream = getOutputPorts().createPort("output stream");
	private OutputPort outputHub = getOutputPorts().createPort("hub");
	private OutputPort outputExperimentResult = getOutputPorts().createPort("experiment result");

	private final static String PARAM_KEY_WINDOW_SIZE = "Sliding Window Size";
	private final static String PARAM_DESC_WINDOW_SIZE = "Set the size of the sliding window adopted on the stream";
	private final static int PARAM_DEFAULT_WINDOW_SIZE = 1000;

	private final static String PARAM_KEY_IS_EXPERIMENT = "Experiment";
	private final static String PARAM_DESC_IS_EXPERIMENT = "Indicator variable to indicate whether we are running this operator inn a controlled experimental setting";
	private final static boolean PARAM_DEFAULT_IS_EXPERIMENT = false;

	private final static String PARAM_KEY_EMISSION_DELAY = "Emission Delay";
	private final static String PARAM_DESC_EMISSION_DELAY = "Specify an emission delay for the filtering of events.";
	private final static int PARAM_DEFAULT_EMISSION_DELAY = 0;

	private final static String PARAM_KEY_INCLUSION_THRESHOLD = "Inclusion Threshold";
	private final static String PARAM_DESC_INCLUSION_THRESHOLD = "Threshold to be applied for filtering";
	private final static double PARAM_DEFAULT_INCLUSION_THRESHOLD = 0.2;

	private final static String PARAM_KEY_MAX_LOOKAHEAD = "Maximum Look-ahead";
	private final static String PARAM_DESC_MAX_LOOKAHEAD = "In case we decide to label an event as being spurious, we are allowed to look-ahead / back in the trace to decide whether or not the label relates to parallel behaviour.";
	private final static byte PARAM_DEFAULT_MAX_LOOKAHEAD = 3;

	private final static String PARAM_KEY_NOISE_LABEL_KEY = "Noise Label Key";
	private final static String PARAM_DESC_NOISE_LABEL_KEY = "In case of an experiment, this indicates the data packet key which allows us to classify the event as either being noise or not. Note that the stream generator always prepends xsevent:data: to the actual key within the input event log/stream";
	private final static String PARAM_DEFAULT_NOISE_LABEL_KEY = "xsevent:data:noise";

	private final static String PARAM_KEY_NOISE_LABEL_VALUE = "Noise Label Value";
	private final static String PARAM_DESC_NOISE_LABEL_VALUE = "In case of an experiment, this indicates the data packet attribute value which allows us to classify the event as either being noise or not";
	private final static String PARAM_DEFAULT_NOISE_LABEL_VALUE = "true";

	private final static String PARAM_KEY_FILTER_TECHNIQUE = "Filter Technique";
	private final static String PARAM_DESC_FILTER_TEHCNIQUE = "Allows us to determine what filter strategy to use, i.e. what window to incorporate.";
	private final static FilteringMethod[] PARAM_VALUES_FILTER_TECHNIQUE = EnumSet.allOf(FilteringMethod.class)
			.toArray(new FilteringMethod[EnumSet.allOf(FilteringMethod.class).size()]);

	private final static String PARAM_KEY_ADJUSTMENT_METHOD = "Adjustment Method";
	private final static String PARAM_DESC_ADJUSTMENT_METHOD = "Allows us to determine how to internally filter.";
	private final static AdjustmentMethod[] PARAM_VALUES_ADJUSTMENT_METHOD = EnumSet.allOf(AdjustmentMethod.class)
			.toArray(new AdjustmentMethod[EnumSet.allOf(AdjustmentMethod.class).size()]);

	private final static String PARAM_KEY_ABSTRACTION = "Abstraction";
	private final static String PARAM_DESC_ABSTRACTION = "What abstraction to use to define the current state in the Markov chain / conditional part of probability.";
	private final static Abstraction[] PARAM_VALUES_ABSTRACTION = EnumSet.allOf(Abstraction.class)
			.toArray(new Abstraction[EnumSet.allOf(Abstraction.class).size()]);

	private final short TIME_OUT = 1000;

	private final String COLUMN_TRUE_POSITIVE = "true_positive";
	private final String COLUMN_FALSE_POSITIVE = "false_positive";
	private final String COLUMN_TRUE_NEGATIVE = "true_negative";
	private final String COLUMN_FALSE_NEGATIVE = "false_negative";
	private final String COLUMN_RECALL = "recall";
	private final String COLUMN_PRECISION = "precision";
	private final String COLUMN_SPECIFITY = "specifity";
	private final String COLUMN_NPV = "npv";
	private final String COLUMN_ACCURACY = "accuracy";
	private final String COLUMN_F1_SCORE = "f1_score";

	private final String[] QUALITY_METRICS_COLUMN_NAMES = new String[] { COLUMN_TRUE_POSITIVE, COLUMN_FALSE_POSITIVE,
			COLUMN_TRUE_NEGATIVE, COLUMN_FALSE_NEGATIVE, COLUMN_RECALL, COLUMN_PRECISION, COLUMN_SPECIFITY, COLUMN_NPV,
			COLUMN_ACCURACY, COLUMN_F1_SCORE };

	private final int[] QUALITY_METRICS_COLUMN_TYPES = new int[] { Ontology.INTEGER, Ontology.INTEGER, Ontology.INTEGER,
			Ontology.INTEGER, Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.REAL,
			Ontology.REAL };

	private final String[] QUALITY_METRICS_COLUMN_ROLES = new String[] { AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR };

	public TrieBasedSpuriousEventFilterOperatorImpl(OperatorDescription description) {
		super(description);
		getTransformer().addGenerationRule(outputStream, XSEventStreamIOObject.class);
		getTransformer().addGenerationRule(outputHub, XSHubIOObject.class);
		ExampleSetMetaData esmd = new ExampleSetMetaData();
		for (int attIndex = 0; attIndex < QUALITY_METRICS_COLUMN_NAMES.length; attIndex++) {
			AttributeMetaData amd = new AttributeMetaData(QUALITY_METRICS_COLUMN_NAMES[attIndex],
					QUALITY_METRICS_COLUMN_TYPES[attIndex]);
			amd.setRegular();
			amd.setNumberOfMissingValues(new MDInteger(0));
			esmd.addAttribute(amd);
			getTransformer().addRule(new GenerateNewMDRule(outputExperimentResult, esmd));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doWork() throws UserError {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "start do work filter spurious events");
		StreamBasedEventLogParametersImpl storageParams = new StreamBasedEventLogParametersImpl();
		storageParams.setSlidingWindowSize(getParameterAsInt(PARAM_KEY_WINDOW_SIZE));
		ConditionalProbabilitiesBasedXSEventFilterParametersImpl filterParams = new ConditionalProbabilitiesBasedXSEventFilterParametersImpl();
		filterParams.setAbstraction(PARAM_VALUES_ABSTRACTION[getParameterAsInt(PARAM_KEY_ABSTRACTION)]);
		filterParams.setFiltermethod(PARAM_VALUES_FILTER_TECHNIQUE[getParameterAsInt(PARAM_KEY_FILTER_TECHNIQUE)]);
		filterParams
				.setAdjustmentmethod(PARAM_VALUES_ADJUSTMENT_METHOD[getParameterAsInt(PARAM_KEY_ADJUSTMENT_METHOD)]);
		filterParams.setMaxPatternLength(getParameterAsInt(PARAM_KEY_MAX_LOOKAHEAD));
		filterParams.setCutoffThreshold(getParameterAsDouble(PARAM_KEY_INCLUSION_THRESHOLD));
		filterParams.setExperiment(getParameterAsBoolean(PARAM_KEY_IS_EXPERIMENT));
		filterParams.setNoiseClassificationLabelKey(getParameterAsString(PARAM_KEY_NOISE_LABEL_KEY));
		filterParams.setNoiseClassificationLabelValue(getParameterAsString(PARAM_KEY_NOISE_LABEL_VALUE));
		ConditionalProbabilitiesBasedXSEventFilterImpl hub = new ConditionalProbabilitiesBasedXSEventFilterImpl(
				filterParams, storageParams);
		XSEventStream out = XSEventStreamFactory.createXSEventStream(CommunicationType.SYNC);
		out.start();
		out.connect(hub);
		hub.start();
		inputEventStream.getData(XSEventStreamIOObject.class).getArtifact().connect(hub);
		if (getParameterAsBoolean(PARAM_KEY_IS_EXPERIMENT)) {
			XSAuthor<XSEvent> streamAuthor = (XSAuthor<XSEvent>) inputAuthor.getData(XSAuthorIOObject.class)
					.getArtifact();
			streamAuthor.start();
			ExampleSet stats = inputStreamStatistics.getData(ExampleSet.class);
			// TODO: make statistics generic
			double maxEvent = stats.getExample(0).getValue(
					stats.getAttributes().get(XLogToEventStreamOperatorImpl.STATISTICS_COLUMN_NAME_NUM_EVENTS));
			while (hub.getNumberOfPacketsReceived() < maxEvent) {
				try {
					Thread.sleep(TIME_OUT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			Attribute[] attributes = new Attribute[QUALITY_METRICS_COLUMN_NAMES.length];
			for (int i = 0; i < attributes.length; i++) {
				attributes[i] = AttributeFactory.createAttribute(QUALITY_METRICS_COLUMN_NAMES[i],
						QUALITY_METRICS_COLUMN_TYPES[i]);
			}
			ExampleSetBuilder builder = ExampleSets.from(attributes);
			DataRowFactory dataRowFactory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
			Object[] values = new Object[QUALITY_METRICS_COLUMN_NAMES.length];
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_TRUE_POSITIVE)] = hub.getTruePositives();
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_FALSE_POSITIVE)] = hub.getFalsePositives();
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_TRUE_NEGATIVE)] = hub.getTrueNegatives();
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_FALSE_NEGATIVE)] = hub.getFalseNegatives();
			final double recall = ((double) hub.getTruePositives())
					/ ((double) (hub.getTruePositives() + hub.getFalseNegatives()));
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_RECALL)] = recall;
			final double precision = ((double) hub.getTruePositives())
					/ ((double) (hub.getTruePositives() + hub.getFalsePositives()));
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_PRECISION)] = precision;
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES,
					COLUMN_SPECIFITY)] = ((double) hub.getTrueNegatives())
							/ ((double) (hub.getTrueNegatives() + hub.getFalsePositives()));
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_NPV)] = ((double) hub.getTrueNegatives())
					/ ((double) (hub.getTrueNegatives() + hub.getFalseNegatives()));
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES,
					COLUMN_ACCURACY)] = ((double) hub.getTruePositives() + hub.getTrueNegatives())
							/ ((double) (hub.getTruePositives() + hub.getTrueNegatives() + hub.getFalsePositives()
									+ hub.getFalseNegatives()));
			values[ArrayUtils.indexOf(QUALITY_METRICS_COLUMN_NAMES, COLUMN_F1_SCORE)] = (2 * recall * precision)
					/ (recall + precision);
			
			builder.addDataRow(dataRowFactory.create(values, attributes));
			outputExperimentResult.deliver(builder.build());
			// shutdown that shit
			streamAuthor.interrupt();
			((XSEventStreamIOObject) inputEventStream.getAnyDataOrNull()).getArtifact().interrupt();
			hub.interrupt();
			out.interrupt();
		}
		outputStream.deliver(new XSEventStreamIOObject(out, RapidProMGlobalContext.instance().getPluginContext()));
		outputHub.deliver(
				new XSHubIOObject<XSEvent, XSEvent>(hub, RapidProMGlobalContext.instance().getPluginContext()));
		logger.log(Level.INFO, "end do work filter spurious events");
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		ParameterTypeInt slidingWindowSize = new ParameterTypeInt(PARAM_KEY_WINDOW_SIZE, PARAM_DESC_WINDOW_SIZE, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_WINDOW_SIZE, false);
		params.add(slidingWindowSize);

		ParameterTypeCategory abstraction = new ParameterTypeCategory(PARAM_KEY_ABSTRACTION, PARAM_DESC_ABSTRACTION,
				ObjectUtils.toString(PARAM_VALUES_ABSTRACTION), 0, false);
		params.add(abstraction);

		ParameterTypeCategory filteringMethod = new ParameterTypeCategory(PARAM_KEY_FILTER_TECHNIQUE,
				PARAM_DESC_FILTER_TEHCNIQUE, ObjectUtils.toString(PARAM_VALUES_FILTER_TECHNIQUE), 0, false);
		params.add(filteringMethod);

		ParameterTypeCategory adjustment = new ParameterTypeCategory(PARAM_KEY_ADJUSTMENT_METHOD,
				PARAM_DESC_ADJUSTMENT_METHOD, ObjectUtils.toString(PARAM_VALUES_ADJUSTMENT_METHOD), 0, false);
		params.add(adjustment);

		ParameterTypeDouble inclusionThreshold = new ParameterTypeDouble(PARAM_KEY_INCLUSION_THRESHOLD,
				PARAM_DESC_INCLUSION_THRESHOLD, 0, 1, PARAM_DEFAULT_INCLUSION_THRESHOLD, false);
		params.add(inclusionThreshold);

		ParameterTypeInt maxLookAhead = new ParameterTypeInt(PARAM_KEY_MAX_LOOKAHEAD, PARAM_DESC_MAX_LOOKAHEAD, 0,
				Byte.MAX_VALUE, PARAM_DEFAULT_MAX_LOOKAHEAD, false);
		params.add(maxLookAhead);

		ParameterTypeInt emissionDelay = new ParameterTypeInt(PARAM_KEY_EMISSION_DELAY, PARAM_DESC_EMISSION_DELAY, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_EMISSION_DELAY, true);
		params.add(emissionDelay);

		ParameterTypeBoolean isExperiment = new ParameterTypeBoolean(PARAM_KEY_IS_EXPERIMENT, PARAM_DESC_IS_EXPERIMENT,
				PARAM_DEFAULT_IS_EXPERIMENT, true);
		params.add(isExperiment);

		ParameterTypeString noiseKeyVal = new ParameterTypeString(PARAM_KEY_NOISE_LABEL_KEY, PARAM_DESC_NOISE_LABEL_KEY,
				PARAM_DEFAULT_NOISE_LABEL_KEY, true);
		noiseKeyVal
				.registerDependencyCondition(new BooleanParameterCondition(this, PARAM_KEY_IS_EXPERIMENT, true, true));
		params.add(noiseKeyVal);

		ParameterTypeString noiseLabelVal = new ParameterTypeString(PARAM_KEY_NOISE_LABEL_VALUE,
				PARAM_DESC_NOISE_LABEL_VALUE, PARAM_DEFAULT_NOISE_LABEL_VALUE, true);
		noiseLabelVal
				.registerDependencyCondition(new BooleanParameterCondition(this, PARAM_KEY_IS_EXPERIMENT, true, true));
		params.add(noiseLabelVal);

		return params;

	}

}
