package org.rapidprom.operators.abstraction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.model.XLog;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.log.utils.XUtils;
import org.processmining.logenhancement.abstraction.PatternBasedLogAbstractionPlugin;
import org.processmining.logenhancement.abstraction.PatternStructureException;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.logenhancement.abstraction.model.AbstractionPattern;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.DataElement;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration.ControlFlowStorageHandlerType;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration.DataStateStorageHandlerType;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration.UnassignedMode;
import org.processmining.plugins.balancedconformance.controlflow.ControlFlowAlignmentException;
import org.processmining.plugins.balancedconformance.controlflow.adapter.SearchMethod;
import org.processmining.plugins.balancedconformance.dataflow.DataAlignmentAdapter.ILPSolver;
import org.processmining.plugins.balancedconformance.dataflow.exception.DataAlignmentException;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.processmining.xesalignmentextension.XAlignmentExtension.MoveType;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignedLog;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignment;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignmentMove;
import org.rapidprom.exceptions.ExampleSetReaderException;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.TransEvMappingIOObject;
import org.rapidprom.ioobjects.XAlignedLogIOObject;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.conformance.util.AlignmentCostIO;
import org.rapidprom.operators.conformance.util.DataAlignmentCostIO;
import org.rapidprom.operators.conformance.util.VariableMappingIO;
import org.rapidprom.operators.util.RapidProMProgress;
import org.rapidprom.util.ObjectUtils;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ProcessSetupError.Severity;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimpleMetaDataError;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeLong;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;

/**
 * Implements the Pattern-based abstraction technique presented in the BPM'16
 * paper: doi:10.1007/978-3-319-45348-4_8
 * 
 * @author F. Mannhardt
 *
 */
public class AbstractLogBasedOnAbstractionModelOperator extends Operator {

	private static final String COST_LOG_MOVE_KEY = "Cost: Log move";
	private static final String COST_MODEL_MOVE_KEY = "Cost: Model move";
	private static final String COST_MISSING_WRITE_KEY = "Cost: Missing Write";
	private static final String COST_WRONG_WRITE_KEY = "Cost: Wrong Write";
	private static final String CONCURRENT_THREADS_KEY = "Number of Threads",
			CONCURRENT_THREADS_DESCR = "Specify the number of threads used to calculate alignments in parallel."
					+ " With each extra thread, more memory is used but less cpu time is required.",
			KEEP_UNMAPPED_EVENTS_KEY = "Keep Unmapped Events",
			KEEP_UNMAPPED_EVENTS_DESCR = "Do you want to keep low-level events that cannot be mapped to any activity pattern? "
					+ "This can be useful if your event log already contains some events at the desired level of abstraction.",
			ERROR_RATE_KEY = "Error Rate", ERROR_RATE_DESCR = "Specify the acceptable 'matching error' ([0.0,1.0]";

	private static final String MAX_QUEUE = "Maximum queue size";
	private static final String MAX_TIME = "Maximum time per trace";

	private static final String SEARCH_METHOD = "Search method";
	private static final String MILP_SOLVER = "MILP Solver";

	private static final String KEEP_DATA_SEARCH_SPACE = "Share data search space";
	private static final String KEEP_CONTROL_FLOW_SEARCH_SPACE = "Share control-flow search space";

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputAbstractionModel = getInputPorts().createPort("abstraction model (ProM Abstraction Model)",
			AbstractionModelIOObject.class);

	private InputPort inputTransitionMapping = getInputPorts()
			.createPort("mapping (ProM Transition/Event Class Mapping)");
	private InputPort inputVariableMapping = getInputPorts().createPort("variable mapping (Example set)");

	private InputPort inputCosts = getInputPorts().createPort("costs control-flow (Example set)");
	private InputPort inputCostsData = getInputPorts().createPort("costs data (Example set)");

	private OutputPort outputLog = getOutputPorts().createPort("abstracted event log (ProM Event Log)");
	private OutputPort outputQualityMeasure = getOutputPorts().createPort("quality measures (Example set)");
	private OutputPort outputAlignedLog = getOutputPorts().createPort("aligned log (ProM Aligned Event Log)");
	private OutputPort passthroughAbstractionModel = getOutputPorts()
			.createPort("abstraction model (ProM Abstraction Model)");
	private OutputPort passthroughLog = getOutputPorts().createPort("event log (ProM Event Log)");
	private OutputPort passthroughTransitionMapping = getOutputPorts().createPort("mapping (Example set)");
	private OutputPort passthroughVariableMapping = getOutputPorts().createPort("variable mapping (Example set)");
	private OutputPort passthroughCosts = getOutputPorts().createPort("costs control-flow (Example set)");
	private OutputPort passthroughCostsData = getOutputPorts().createPort("costs data (Example set)");

	public AbstractLogBasedOnAbstractionModelOperator(OperatorDescription description) {
		super(description);
		inputTransitionMapping.addPrecondition(
				new SimplePrecondition(inputTransitionMapping, new MetaData(TransEvMappingIOObject.class), true));
		inputVariableMapping
				.addPrecondition(new SimplePrecondition(inputVariableMapping, new MetaData(ExampleSet.class), false));
		inputCosts.addPrecondition(new SimplePrecondition(inputCosts, new MetaData(ExampleSet.class), false));
		inputCostsData.addPrecondition(new SimplePrecondition(inputCostsData, new MetaData(ExampleSet.class), false));

		getTransformer().addRule(new GenerateNewMDRule(outputLog, XLogIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputQualityMeasure, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(outputAlignedLog, XAlignedLogIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughAbstractionModel, AbstractionModelIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughLog, XLogIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughVariableMapping, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughTransitionMapping, TransEvMappingIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughCosts, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughCostsData, ExampleSet.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog log = getXLog();
		AbstractionModel abstractionModel = getAbstractionModel();

		try {
			boolean keepUnmappedEvents = getParameterAsBoolean(KEEP_UNMAPPED_EVENTS_KEY);
			double errorRateLimit = getParameterAsDouble(ERROR_RATE_KEY);

			TransEvClassMapping transitionMapping = getTransitionMapping();

			DataPetriNetsWithMarkings model = abstractionModel.getCombinedDPN();
			BalancedProcessorConfiguration config = BalancedProcessorConfiguration.newDefaultInstance(model,
					model.getInitialMarking(), model.getFinalMarkings(), log, transitionMapping.getEventClassifier(),
					getDefaultCostLogMove(), getDefaultCostModelMove(), getDefaultCostMissingWrite(),
					getDefaultCostWrongWrite());
			applyUserDefinedTransitionMapping(transitionMapping, config);

			config.setActivateDataViewCache(false);

			config.setSearchMethod(getSearchMethod());
			config.setIlpSolver(getMILPSolver());
			config.setMaxQueuedStates(getMaxQueuedStates());
			config.setTimeLimitPerTrace(getMaxTimePerTrace());

			config.setKeepControlFlowSearchSpace(getKeepControlFlowSearchSpace());
			config.setKeepDataFlowSearchSpace(getKeepDataFlowSearchSpace());

			// Don't share data structure beteen threads when running single threaded
			if (config.getConcurrentThreads() == 1) {
				config.setKeepDataFlowSearchSpace(false);
				config.setKeepControlFlowSearchSpace(false);
			}

			// Use more efficient data structures without locking in these cases
			if (!config.isKeepControlFlowSearchSpace()) {
				config.setControlFlowStorageHandler(ControlFlowStorageHandlerType.MEMORY_EFFICIENT);
			}
			if (!config.isKeepDataFlowSearchSpace()) {
				config.setDataStateStorageHandler(DataStateStorageHandlerType.PRIMITIVE_NOLOCK);				
			}
			
			if (inputVariableMapping.isConnected()) {
				try {
					applyUserDefinedVariableMapping(getVariableMapping(), config);
				} catch (ExampleSetReaderException e) {
					inputVariableMapping
							.addError(new SimpleMetaDataError(Severity.WARNING, inputVariableMapping, e.getMessage()));
				}
			}

			if (inputCosts.isConnected()) {
				try {
					applyUserDefinedCosts(getCostsControlFlow(), log, model, config);
				} catch (ExampleSetReaderException e) {
					inputCosts.addError(new SimpleMetaDataError(Severity.WARNING, inputCosts, e.getMessage()));
				}
			}

			if (inputCostsData.isConnected()) {
				try {
					applyUserDefinedDataCosts(getCostsData(), log, model, config);
				} catch (ExampleSetReaderException e) {
					inputCostsData.addError(new SimpleMetaDataError(Severity.WARNING, inputCostsData, e.getMessage()));
				}
			}

			passthroughAbstractionModel.deliver(inputAbstractionModel.getData(AbstractionModelIOObject.class));
			passthroughLog.deliver(inputXLog.getData(XLogIOObject.class));
			passthroughCosts.deliver(new AlignmentCostIO().writeCostsToExampleSet(config.getMapEvClass2Cost(),
					config.getMapTrans2Cost()));
			passthroughCostsData
					.deliver(new DataAlignmentCostIO().writeCostsToExampleSet(config.getVariableCost()));
			passthroughVariableMapping
					.deliver(new VariableMappingIO().writeVariableMapping(config.getVariableMapping()));
			passthroughTransitionMapping.deliver(inputTransitionMapping.getData(TransEvMappingIOObject.class));

			XAlignedLog alignedLog = PatternBasedLogAbstractionPlugin.alignLogToAbstractionModel(
					new RapidProMProgress(getProgress()), config, log, abstractionModel);

			XLog abstractedLog = PatternBasedLogAbstractionPlugin.abstractAlignedLog(abstractionModel, alignedLog,
					keepUnmappedEvents, errorRateLimit, transitionMapping);

			// TODO currently this need to be done after the abstraction -
			// REFACTOR
			ExampleSet qualityMeasureTable = createQualityMeasureTable(alignedLog, abstractionModel);
			outputQualityMeasure.deliver(qualityMeasureTable);

			outputLog.deliver(new XLogIOObject(abstractedLog, getContext()));
			outputAlignedLog.deliver(new XAlignedLogIOObject(alignedLog.getLog(), getContext()));

		} catch (ControlFlowAlignmentException | DataAlignmentException | PatternStructureException e) {
			throw new OperatorException("Failed abstraction!", e);
		}

	}

	// TODO refactor in LogEnhancement
	private ExampleSet createQualityMeasureTable(XAlignedLog alignedLog, AbstractionModel abstractionModel) {

		Attribute nameAttr = AttributeFactory.createAttribute("Name", Ontology.STRING);
		Attribute patternAttr = AttributeFactory.createAttribute("Pattern", Ontology.STRING);
		Attribute valueAttr = AttributeFactory.createAttribute("Value", Ontology.NUMERICAL);
		MemoryExampleTable table = new MemoryExampleTable(nameAttr, patternAttr, valueAttr);

		Attribute[] attributes = new Attribute[] { nameAttr, patternAttr, valueAttr };
		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		Map<String, AbstractionPattern> transitionIdToPattern = new HashMap<>();

		for (AbstractionPattern pattern : abstractionModel.getPatterns()) {
			long matchCount = 0;
			long moveCount = 0;
			long modelMoveCount = 0;
			for (String id : pattern.getTransitionsAsLocalIds()) {
				transitionIdToPattern.put(id, pattern);
			}
			Transition startTransition = pattern.getStartTransition();
			Set<String> transitions = pattern.getTransitionsAsLocalIds();
			String startTransitionId = startTransition.getLocalID().toString();
			for (XAlignment alignment : alignedLog) {
				for (XAlignmentMove move : alignment) {
					String activityId = move.getActivityId();
					if (startTransitionId.equals(activityId)) {
						matchCount++;
					}
					if (move.isObservable() && transitions.contains(move.getActivityId())) {
						moveCount++;
						if (move.getType() != MoveType.SYNCHRONOUS) {
							modelMoveCount++;
						}
					}
				}
			}

			table.addDataRow(
					factory.create(new Object[] { "numMatches", pattern.getPatternName(), matchCount }, attributes));

			if (moveCount != 0) {
				double error = modelMoveCount / (double) moveCount;
				table.addDataRow(
						factory.create(new Object[] { "matchingError", pattern.getPatternName(), error }, attributes));
			} else {
				table.addDataRow(
						factory.create(new Object[] { "matchingError", pattern.getPatternName(), 0 }, attributes));
			}
		}

		return table.createExampleSet();
	}

	private void applyUserDefinedTransitionMapping(TransEvClassMapping mapping,
			BalancedProcessorConfiguration alignmentConfig) {
		alignmentConfig.setActivityMapping(mapping);
		// TODO fix in DataAwareReplayer
		alignmentConfig.getMapEvClass2Cost().put(mapping.getDummyEventClass(), 0);
	}

	private void applyUserDefinedVariableMapping(Map<String, String> variableMapping,
			BalancedProcessorConfiguration alignmentConfig) {
		for (Entry<String, String> entry : variableMapping.entrySet()) {
			alignmentConfig.getVariableMapping().put(entry.getKey(), entry.getValue());
		}
	}

	private void applyUserDefinedCosts(ExampleSet controlFlowCosts, XLog log, DataPetriNetsWithMarkings model,
			BalancedProcessorConfiguration alignmentConfig) throws UserError, ExampleSetReaderException {
		AlignmentCostIO costReader = new AlignmentCostIO();
		XEventClasses eventClasses = XUtils
				.createEventClasses(alignmentConfig.getActivityMapping().getEventClassifier(), log);
		Map<String, Transition> transitions = getTransitions(model);
		costReader.readCostsFromExampleSet(controlFlowCosts, eventClasses, transitions,
				alignmentConfig.getMapEvClass2Cost(), alignmentConfig.getMapTrans2Cost());
	}

	private void applyUserDefinedDataCosts(ExampleSet dataCosts, XLog log, DataPetriNetsWithMarkings model,
			BalancedProcessorConfiguration alignmentConfig) throws UserError, ExampleSetReaderException {
		DataAlignmentCostIO costReader = new DataAlignmentCostIO();
		Set<String> variables = new HashSet<>();
		for (DataElement element : model.getVariables()) {
			variables.add(element.getVarName());
		}
		costReader.readCostsFromExampleSet(dataCosts, getDefaultCostWrongWrite(), getDefaultCostMissingWrite(),
				model.getTransitions(), variables);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeBoolean(KEEP_UNMAPPED_EVENTS_KEY, KEEP_UNMAPPED_EVENTS_DESCR, true, false));
		params.add(new ParameterTypeDouble(ERROR_RATE_KEY, ERROR_RATE_DESCR, 0.0, 1.0, 1.0));

		// alignment config

		params.add(new ParameterTypeInt(CONCURRENT_THREADS_KEY, CONCURRENT_THREADS_DESCR, 1,
				Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), true));
		params.add(new ParameterTypeInt(COST_WRONG_WRITE_KEY, "Default cost for a wrong write operation.", 0, 100, 1));
		params.add(
				new ParameterTypeInt(COST_MISSING_WRITE_KEY, "Default cost for a missing write operation.", 0, 100, 1));
		params.add(new ParameterTypeInt(COST_MODEL_MOVE_KEY, "Default cost for a model move.", 0, 100, 1));
		params.add(new ParameterTypeInt(COST_LOG_MOVE_KEY, "Default cost for a log move.", 0, 100, 1));

		params.add(new ParameterTypeCategory(SEARCH_METHOD, "Optimal alignment search method.",
				ObjectUtils.toString(SearchMethod.values()), SearchMethod.ASTAR_GRAPH.ordinal(), true));
		params.add(new ParameterTypeCategory(MILP_SOLVER, "MILP solver that should be used for the data perspective.",
				ObjectUtils.toString(ILPSolver.values()), ILPSolver.ILP_LPSOLVE.ordinal(), true));

		params.add(new ParameterTypeBoolean(KEEP_DATA_SEARCH_SPACE,
				"Share the explored data search space (i.e., values of variables) between traces. This may speed up the computation but uses more memory.",
				true, true));
		params.add(new ParameterTypeBoolean(KEEP_CONTROL_FLOW_SEARCH_SPACE,
				"Share the explored control-flow search space (i.e., marking and parikh vector) between traces. This may speed up the computation but uses more memory.",
				true, true));

		params.add(
				new ParameterTypeInt(MAX_QUEUE, "Maximum queue size", 1, Integer.MAX_VALUE, Integer.MAX_VALUE, true));
		params.add(new ParameterTypeLong(MAX_TIME, "Maximum time per trace (seconds)", 1, Long.MAX_VALUE,
				Long.MAX_VALUE, true));

		return params;
	}

	private int getDefaultCostMissingWrite() throws UndefinedParameterError {
		return getParameterAsInt(COST_MISSING_WRITE_KEY);
	}

	private int getDefaultCostWrongWrite() throws UndefinedParameterError {
		return getParameterAsInt(COST_WRONG_WRITE_KEY);
	}

	private int getDefaultCostModelMove() throws UndefinedParameterError {
		return getParameterAsInt(COST_MODEL_MOVE_KEY);
	}

	private int getDefaultCostLogMove() throws UndefinedParameterError {
		return getParameterAsInt(COST_LOG_MOVE_KEY);
	}

	private ExampleSet getCostsControlFlow() throws UserError {
		return inputCosts.getData(ExampleSet.class);
	}

	private ExampleSet getCostsData() throws UserError {
		return inputCostsData.getData(ExampleSet.class);
	}

	private Map<String, Transition> getTransitions(PetrinetGraph model) {
		Map<String, Transition> transitions = new HashMap<>();
		for (Transition t : model.getTransitions()) {
			transitions.put(t.getLabel(), t);
		}
		return transitions;
	}

	private TransEvClassMapping getTransitionMapping() throws UserError {
		return inputTransitionMapping.getData(TransEvMappingIOObject.class).getArtifact();
	}

	private Map<String, String> getVariableMapping() throws UserError, ExampleSetReaderException {
		return new VariableMappingIO().readVariableMapping(inputVariableMapping.getData(ExampleSet.class));
	}

	private AbstractionModel getAbstractionModel() throws UserError {
		return inputAbstractionModel.getData(AbstractionModelIOObject.class).getArtifact();
	}

	private XLog getXLog() throws UserError {
		return inputXLog.getData(XLogIOObject.class).getArtifact();
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}
	
	private SearchMethod getSearchMethod() throws UndefinedParameterError {
		for (SearchMethod method : SearchMethod.values()) {
			if (method.toString().equals(getParameterAsString(SEARCH_METHOD))) {
				return method;
			}
		}
		return SearchMethod.ASTAR_GRAPH;
	}

	private ILPSolver getMILPSolver() throws UndefinedParameterError {
		for (ILPSolver solver : ILPSolver.values()) {
			if (solver.toString().equals(getParameterAsString(MILP_SOLVER))) {
				return solver;
			}
		}
		return ILPSolver.ILP_LPSOLVE;
	}
	
	private boolean getKeepDataFlowSearchSpace() {
		return getParameterAsBoolean(KEEP_DATA_SEARCH_SPACE);
	}

	private boolean getKeepControlFlowSearchSpace() {
		return getParameterAsBoolean(KEEP_CONTROL_FLOW_SEARCH_SPACE);
	}


	private int getMaxQueuedStates() throws UndefinedParameterError {
		return getParameterAsInt(MAX_QUEUE);
	}

	private long getMaxTimePerTrace() throws UndefinedParameterError {
		return getParameterAsLong(MAX_TIME);
	}

}