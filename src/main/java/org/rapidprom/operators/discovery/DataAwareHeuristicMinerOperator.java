package org.rapidprom.operators.discovery;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.model.XLog;
import org.processmining.dataawarecnetminer.mining.data.DataAwareCausalGraphBuilder;
import org.processmining.dataawarecnetminer.mining.data.DataAwareCausalGraphBuilder.DataAwareCausalGraphConfig;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicCausalNetMiner;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicsCausalGraphMiner;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicCausalNetMiner.CausalNetConfig;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicsCausalGraphBuilder.HeuristicsConfig;
import org.processmining.dataawarecnetminer.model.DataAwareCausalGraph;
import org.processmining.dataawarecnetminer.model.DataRelationStorage;
import org.processmining.dataawarecnetminer.model.DependencyAwareCausalGraph;
import org.processmining.dataawarecnetminer.model.EventRelationStorage;
import org.processmining.dataawarecnetminer.model.FrequencyAwareCausalNet;
import org.processmining.dataawarecnetminer.util.DataDiscoveryUtil;
import org.processmining.datadiscovery.DecisionTreeConfig;
import org.processmining.datadiscovery.RuleDiscoveryException;
import org.processmining.models.cnet.CausalNet;
import org.rapidprom.exceptions.ExampleSetReaderException;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.CausalNetIOObject;
import org.rapidprom.ioobjects.HeuristicsNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.google.common.collect.ImmutableSet;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ProcessSetupError.Severity;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.operator.ports.metadata.SimpleMetaDataError;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;

/**
 * 
 * 
 * 
 * @author F. Mannhardt
 *
 */
public class DataAwareHeuristicMinerOperator extends AbstractRapidProMDiscoveryOperator {

	public static final String PARAMETER_RELATIVE_TO_BEST = "Heuristic: Relative-to-best threshold",
			PARAMETER_RELATIVE_TO_BEST_DESCR = "Admissable distance between directly follows relations for an "
					+ "activity and the activity's best one. At 0 only the best directly follows "
					+ "relation will be shown for every activity, at 1 all will be shown.",
			PARAMETER_DEPENDENCY_THRESHOLD = "Heuristic: Dependency threshold",
			PARAMETER_DEPENDENCY_THRESHOLD_DESCR = "Strength of the directly follows relations determines when to "
					+ "Show arcs (based on how frequently one activity is followed by another).",
			PARAMETER_L1_THRESHOLD = "Heuristic: Length-one-loops threshold",
			PARAMETER_L1_THRESHOLD_DESCR = "Show arcs based on frequency of L1L observations",
			PARAMETER_L2_THRESHOLD = "Heuristic: Length-two-loops threshold",
			PARAMETER_L2_THRESHOLD_DESCR = "Show arcs based on frequency of L2L observations",
			PARAMETER_LONG_DISTANCE = "Heuristic: Long distance dependency",
			PARAMETER_LONG_DISTANCE_DESCR = "Consider eventually follows relations in the heuristic discovery.",
			PARAMETER_ALL_TASK_CONNECTED = "Heuristic: Connect all tasks",
			PARAMETER_ALL_TAKS_CONNECTED_DESCR = "Force every task to have at least one input and output arc, "
					+ "except one initial and one final activity.",
			PARAMETER_ACCEPTED_CONNECTED = "Heuristic: Connect accepted",
			PARAMETER_ACCEPTED_CONNECTED_DESCR = "Force every task that is included in the initial dependency graph to have at least one input and one output arc.",
			PARAMETER_OBSERVATION_THRESHOLD = "Heuristic: Frequency of observation",
			PARAMETER_OBSERVATION_THRESHOLD_DESCR = "",
			PARAMETER_BINDINGS_THRESHOLD = "Heuristic: Binding threshold",
			PARAMETER_BINDINGS_THRESHOLD_DESCR = "Strength of input and output binding to be considered for the heuristic discovery of C-net bindings.",
			PARAMETER_CONDITION_THRESHOLD = "Data: Condition",
			PARAMETER_CONDITION_THRESHOLD_DESCR = "Strength of the data conditions that are considered.";

	private static final String ATTRIBUTE_COLUMN = "attribute";

	private static final String PARAMETER_BOOLEAN_SPLIT = "Data: Boolean splits", 
			PARAMETER_BOOLEAN_SPLIT_DESCR = "",
			PARAMETER_CONFIDENCE_THRESHOLD = "Data: Confidence for pruning",
			PARAMETER_CONFIDENCE_THRESHOLD_DESCR = "", 
			PARAMETER_CROSSVALIDATE = "Data: Cross validation",
			PARAMETER_CROSSVALIDATE_DESCR = "Activate 10-times 10-fold stratified cross validation to estimate the quality of data conditions.",
			PARAMETER_MIN_PERCENTAGE_LEAF = "Data: Minimum percentage of instances",
			PARAMETER_MIN_PERCENTAGE_LEAF_DESCR = "The minimum percentage of instances per leaf for the decision tree.",
			PARAMETER_UNPRUNED = "Data: Unpruned tree", 
			PARAMETER_UNPRUNED_DESCR = "", 
			PARAMETER_WEIGHTS = "Data: Use weights",
			PARAMETER_WEIGHTS_DESCR = "Minimize memory usage by using weighted instances. Beneficial for a small set of categorical attributes.";

	private InputPort inputAttributeSelection = getInputPorts().createPort("attribute selection (Example set)");

	private OutputPort outputCausalNet = getOutputPorts().createPort("model (ProM Causal Net)");

	public DataAwareHeuristicMinerOperator(OperatorDescription description) {
		super(description);
		ExampleSetMetaData metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("attribute", Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		inputAttributeSelection.addPrecondition(new SimplePrecondition(inputAttributeSelection, metaData, false));
		getTransformer().addRule(new GenerateNewMDRule(outputCausalNet, HeuristicsNetIOObject.class));
	}

	public void doWork() throws OperatorException {

		XLog log = getXLog();
		XEventClassifier classifier = getXEventClassifier();

		HeuristicsConfig heuristicConfig = getHeuristicsMinerConfig();

		Set<String> selectedAttributes = ImmutableSet.<String> of();
		if (inputAttributeSelection.isConnected()) {
			try {
				ExampleSet attributeSelection = inputAttributeSelection.getData(ExampleSet.class);
				selectedAttributes = readSelectedAttributes(attributeSelection);
			} catch (ExampleSetReaderException e) {
				inputAttributeSelection
						.addError(new SimpleMetaDataError(Severity.WARNING, inputAttributeSelection, e.getMessage()));
			}
		}

		DataAwareCausalGraphConfig graphConfig = getDataHeuristicMinerConfig();
		DecisionTreeConfig discoveryConfig = getDataDiscoveryConfig();
		CausalNetConfig netConfig = getCausalNetConfig();

		CausalNet causalNet;
		try {
			causalNet = mineCausalNet(log, classifier, selectedAttributes, graphConfig, netConfig, heuristicConfig,
					discoveryConfig);
		} catch (RuleDiscoveryException e) {
			throw new OperatorException("Failed discovering Causal Net: " + e.getMessage(), e);
		}

		CausalNetIOObject causalNetIOObject = new CausalNetIOObject(causalNet,
				RapidProMGlobalContext.instance().getPluginContext());
		outputCausalNet.deliver(causalNetIOObject);
	}

	private Set<String> readSelectedAttributes(ExampleSet data) throws ExampleSetReaderException {
		Attribute attributeAttr = data.getAttributes().get(ATTRIBUTE_COLUMN);

		if (attributeAttr == null) {
			throw new ExampleSetReaderException("Missing column 'attribute'!");
		}

		Set<String> attributes = new HashSet<>();

		for (Example element : data) {
			String attribute = element.getValueAsString(attributeAttr);

			if (attribute == null) {
				throw new ExampleSetReaderException("Missing attribute!");
			} else {
				// for specific variable / transition combination
				attributes.add(attribute);
			}
		}
		return attributes;
	}

	private CausalNetConfig getCausalNetConfig() throws UndefinedParameterError {
		CausalNetConfig config = new CausalNetConfig();
		config.setConsiderLongDistanceRelations(getParameterAsBoolean(PARAMETER_LONG_DISTANCE));
		config.setBindingsThreshold(getParameterAsDouble(PARAMETER_BINDINGS_THRESHOLD));
		return config;
	}

	private DecisionTreeConfig getDataDiscoveryConfig() throws UndefinedParameterError {
		DecisionTreeConfig config = new DecisionTreeConfig();
		config.setBinarySplit(getParameterAsBoolean(PARAMETER_BOOLEAN_SPLIT));
		config.setConfidenceTreshold((float) getParameterAsDouble(PARAMETER_CONFIDENCE_THRESHOLD));
		config.setCrossValidate(getParameterAsBoolean(PARAMETER_CROSSVALIDATE));
		config.setMinPercentageObjectsOnLeaf(getParameterAsDouble(PARAMETER_MIN_PERCENTAGE_LEAF));
		config.setUnpruned(getParameterAsBoolean(PARAMETER_UNPRUNED));
		config.setUseWeights(getParameterAsBoolean(PARAMETER_WEIGHTS));
		return config;
	}

	private DataAwareCausalGraphConfig getDataHeuristicMinerConfig() throws UndefinedParameterError {
		DataAwareCausalGraphConfig config = new DataAwareCausalGraphConfig();
		config.setAcceptedTasksConnected(getParameterAsBoolean(PARAMETER_ACCEPTED_CONNECTED));
		config.setAllTasksConnected(getParameterAsBoolean(PARAMETER_ALL_TASK_CONNECTED));
		config.setDependencyThreshold(getParameterAsDouble(PARAMETER_DEPENDENCY_THRESHOLD));
		config.setObservationThreshold(getParameterAsDouble(PARAMETER_OBSERVATION_THRESHOLD));
		config.setRelativeToBestThreshold(getParameterAsDouble(PARAMETER_RELATIVE_TO_BEST));
		config.setGuardThreshold(getParameterAsDouble(PARAMETER_CONDITION_THRESHOLD));
		return config;
	}

	private HeuristicsConfig getHeuristicsMinerConfig() throws UndefinedParameterError {
		HeuristicsConfig config = new HeuristicsConfig();
		config.setAcceptedTasksConnected(getParameterAsBoolean(PARAMETER_ACCEPTED_CONNECTED));
		config.setAllTasksConnected(getParameterAsBoolean(PARAMETER_ALL_TASK_CONNECTED));
		config.setDependencyThreshold(getParameterAsDouble(PARAMETER_DEPENDENCY_THRESHOLD));
		config.setL1Threshold(getParameterAsDouble(PARAMETER_L1_THRESHOLD));
		config.setL2Threshold(getParameterAsDouble(PARAMETER_L2_THRESHOLD));
		config.setObservationThreshold(getParameterAsDouble(PARAMETER_OBSERVATION_THRESHOLD));
		config.setRelativeToBestThreshold(getParameterAsDouble(PARAMETER_RELATIVE_TO_BEST));
		return config;
	}

	protected CausalNet mineCausalNet(XLog log, XEventClassifier classifier, Set<String> selectedAttributes,
			DataAwareCausalGraphConfig dataConfig, CausalNetConfig netConfig, HeuristicsConfig heuristicConfig,
			DecisionTreeConfig discoveryConfig) throws RuleDiscoveryException, OperatorException {

		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		Map<String, Class<?>> attributeTypes = DataDiscoveryUtil.getRelevantAttributeTypes(log);
		if (!attributeTypes.keySet().containsAll(selectedAttributes)) {
			selectedAttributes.removeAll(attributeTypes.keySet());
			throw new OperatorException("Invalid attributes: " + selectedAttributes);
		}

		EventRelationStorage eventRelations = EventRelationStorage.Factory.createEventRelations(log, classifier,
				executor);
		DataRelationStorage dataRelations = DataRelationStorage.Factory.createDataRelations(eventRelations,
				attributeTypes, selectedAttributes, executor);
		dataRelations.setDataDiscoveryConfig(discoveryConfig);

		HeuristicsCausalGraphMiner miner = new HeuristicsCausalGraphMiner(eventRelations);
		miner.setHeuristicsConfig(heuristicConfig);
		DependencyAwareCausalGraph dependencyGraph = miner.mineCausalGraph();

		DataAwareCausalGraphBuilder dataBuilder = new DataAwareCausalGraphBuilder(eventRelations, dataRelations, dataConfig);
		DataAwareCausalGraph dataAwareGraph = dataBuilder.build(dependencyGraph, executor);

		HeuristicCausalNetMiner causalNetMiner = new HeuristicCausalNetMiner(eventRelations);
		causalNetMiner.setConfig(netConfig);
		FrequencyAwareCausalNet causalNet = causalNetMiner.mineCausalNet(dataAwareGraph, executor);

		return causalNet.getCNet();
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeDouble parameter5 = new ParameterTypeDouble(PARAMETER_OBSERVATION_THRESHOLD,
				PARAMETER_OBSERVATION_THRESHOLD_DESCR, 0, 1, 0.1);
		parameterTypes.add(parameter5);

		ParameterTypeDouble parameter2 = new ParameterTypeDouble(PARAMETER_DEPENDENCY_THRESHOLD,
				PARAMETER_DEPENDENCY_THRESHOLD_DESCR, 0, 1, 0.9);
		parameterTypes.add(parameter2);

		ParameterTypeDouble parameterCond = new ParameterTypeDouble(PARAMETER_CONDITION_THRESHOLD,
				PARAMETER_CONDITION_THRESHOLD_DESCR, 0, 1, 0.5);
		parameterTypes.add(parameterCond);

		ParameterTypeDouble parameterBind = new ParameterTypeDouble(PARAMETER_BINDINGS_THRESHOLD,
				PARAMETER_BINDINGS_THRESHOLD_DESCR, 0, 1, 0.1);
		parameterTypes.add(parameterBind);

		ParameterTypeDouble parameter3 = new ParameterTypeDouble(PARAMETER_L1_THRESHOLD, PARAMETER_L1_THRESHOLD_DESCR,
				0, 1, 0.9, true);
		parameterTypes.add(parameter3);

		ParameterTypeDouble parameter4 = new ParameterTypeDouble(PARAMETER_L2_THRESHOLD, PARAMETER_L2_THRESHOLD_DESCR,
				0, 1, 0.9, true);
		parameterTypes.add(parameter4);

		ParameterTypeDouble parameter1 = new ParameterTypeDouble(PARAMETER_RELATIVE_TO_BEST,
				PARAMETER_RELATIVE_TO_BEST_DESCR, 0, 1, 0.05, true);
		parameterTypes.add(parameter1);

		ParameterTypeBoolean parameter7 = new ParameterTypeBoolean(PARAMETER_LONG_DISTANCE,
				PARAMETER_LONG_DISTANCE_DESCR, false, true);
		parameterTypes.add(parameter7);

		ParameterTypeBoolean parameter6 = new ParameterTypeBoolean(PARAMETER_ALL_TASK_CONNECTED,
				PARAMETER_ALL_TAKS_CONNECTED_DESCR, false, true);
		parameterTypes.add(parameter6);

		ParameterTypeBoolean parameter8 = new ParameterTypeBoolean(PARAMETER_ACCEPTED_CONNECTED,
				PARAMETER_ACCEPTED_CONNECTED_DESCR, true);
		parameterTypes.add(parameter8);

		ParameterTypeDouble parameter13 = new ParameterTypeDouble(PARAMETER_MIN_PERCENTAGE_LEAF,
				PARAMETER_MIN_PERCENTAGE_LEAF_DESCR, 0, 1, 0.1);
		parameterTypes.add(parameter13);

		ParameterTypeBoolean parameter9 = new ParameterTypeBoolean(PARAMETER_BOOLEAN_SPLIT,
				PARAMETER_BOOLEAN_SPLIT_DESCR, false);
		parameterTypes.add(parameter9);

		ParameterTypeBoolean parameter10 = new ParameterTypeBoolean(PARAMETER_UNPRUNED, PARAMETER_UNPRUNED_DESCR, false,
				true);
		parameterTypes.add(parameter10);

		ParameterTypeDouble parameter12 = new ParameterTypeDouble(PARAMETER_CONFIDENCE_THRESHOLD,
				PARAMETER_CONFIDENCE_THRESHOLD_DESCR, 0, 1, 0.25, true);
		parameterTypes.add(parameter12);

		ParameterTypeBoolean parameter11 = new ParameterTypeBoolean(PARAMETER_CROSSVALIDATE,
				PARAMETER_CROSSVALIDATE_DESCR, true);
		parameterTypes.add(parameter11);

		ParameterTypeBoolean parameter14 = new ParameterTypeBoolean(PARAMETER_WEIGHTS, PARAMETER_WEIGHTS_DESCR, true);
		parameterTypes.add(parameter14);

		return parameterTypes;
	}

}