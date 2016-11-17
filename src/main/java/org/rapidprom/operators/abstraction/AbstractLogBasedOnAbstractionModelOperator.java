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
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.DataElement;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration.UnassignedMode;
import org.processmining.plugins.balancedconformance.controlflow.ControlFlowAlignmentException;
import org.processmining.plugins.balancedconformance.dataflow.exception.DataAlignmentException;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignedLog;
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

import com.rapidminer.example.ExampleSet;
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
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;

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

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputAbstractionModel = getInputPorts().createPort("abstraction model (ProM Abstraction Model)",
			AbstractionModelIOObject.class);

	private InputPort inputTransitionMapping = getInputPorts()
			.createPort("mapping (ProM Transition/Event Class Mapping)");
	private InputPort inputVariableMapping = getInputPorts().createPort("variable mapping (Example set)");

	private InputPort inputCosts = getInputPorts().createPort("costs control-flow (Example set)");
	private InputPort inputCostsData = getInputPorts().createPort("costs data (Example set)");

	private OutputPort outputLog = getOutputPorts().createPort("event log (ProM Event Log)");
	private OutputPort outputAlignedLog = getOutputPorts().createPort("aligned log (ProM Aligned Event Log)");	
	private OutputPort outputTransitionMapping = getOutputPorts().createPort("mapping (Example set)");
	private OutputPort outputVariableMapping = getOutputPorts().createPort("variable mapping (Example set)");
	private OutputPort outputCosts = getOutputPorts().createPort("costs control-flow (Example set)");
	private OutputPort outputCostsData = getOutputPorts().createPort("costs data (Example set)");

	public AbstractLogBasedOnAbstractionModelOperator(OperatorDescription description) {
		super(description);
		inputTransitionMapping.addPrecondition(
				new SimplePrecondition(inputTransitionMapping, new MetaData(TransEvMappingIOObject.class), true));
		inputVariableMapping
				.addPrecondition(new SimplePrecondition(inputVariableMapping, new MetaData(ExampleSet.class), false));
		inputCosts.addPrecondition(new SimplePrecondition(inputCosts, new MetaData(ExampleSet.class), false));
		inputCostsData.addPrecondition(new SimplePrecondition(inputCostsData, new MetaData(ExampleSet.class), false));
	
		getTransformer().addRule(new GenerateNewMDRule(outputLog, XLogIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputAlignedLog, XAlignedLogIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputVariableMapping, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(outputTransitionMapping, TransEvMappingIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputCosts, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(outputCostsData, ExampleSet.class));
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
			BalancedProcessorConfiguration alignmentConfig = BalancedProcessorConfiguration.newDefaultInstance(model,
					model.getInitialMarking(), model.getFinalMarkings(), log, transitionMapping.getEventClassifier(),
					getDefaultCostLogMove(), getDefaultCostModelMove(), getDefaultCostMissingWrite(),
					getDefaultCostWrongWrite());
			applyUserDefinedTransitionMapping(transitionMapping, alignmentConfig);
			
			alignmentConfig.setActivateDataViewCache(false);
			alignmentConfig.setKeepDataFlowSearchSpace(false);
			alignmentConfig.setKeepControlFlowSearchSpace(true);
			
			if (inputVariableMapping.isConnected()) {
				try {
					applyUserDefinedVariableMapping(getVariableMapping(), alignmentConfig);
				} catch (ExampleSetReaderException e) {
					inputVariableMapping
							.addError(new SimpleMetaDataError(Severity.WARNING, inputVariableMapping, e.getMessage()));
				}
			}

			if (inputCosts.isConnected()) {
				try {
					applyUserDefinedCosts(getCostsControlFlow(), log, model, alignmentConfig);
				} catch (ExampleSetReaderException e) {
					inputCosts.addError(new SimpleMetaDataError(Severity.WARNING, inputCosts, e.getMessage()));
				}
			}

			if (inputCostsData.isConnected()) {
				try {
					applyUserDefinedDataCosts(getCostsData(), log, model, alignmentConfig);
				} catch (ExampleSetReaderException e) {
					inputCostsData.addError(new SimpleMetaDataError(Severity.WARNING, inputCostsData, e.getMessage()));
				}
			}
			
			outputCosts.deliver(new AlignmentCostIO().writeCostsToExampleSet(alignmentConfig.getMapEvClass2Cost(),
					alignmentConfig.getMapTrans2Cost()));
			outputCostsData
					.deliver(new DataAlignmentCostIO().writeCostsToExampleSet(alignmentConfig.getVariableCost()));
			outputVariableMapping
					.deliver(new VariableMappingIO().writeVariableMapping(alignmentConfig.getVariableMapping()));
			outputTransitionMapping.deliver(inputTransitionMapping.getData(TransEvMappingIOObject.class));
			
			XAlignedLog alignedLog = PatternBasedLogAbstractionPlugin.alignLogToAbstractionModel(new RapidProMProgress(getProgress()), alignmentConfig, log, abstractionModel);

			XLog abstractedLog = PatternBasedLogAbstractionPlugin.abstractAlignedLog(abstractionModel, alignedLog, keepUnmappedEvents, errorRateLimit, transitionMapping);

			outputLog.deliver(new XLogIOObject(abstractedLog, getContext()));

		} catch (ControlFlowAlignmentException | DataAlignmentException | PatternStructureException e) {
			throw new OperatorException("Failed abstraction!", e);
		}

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
		params.add(new ParameterTypeInt(CONCURRENT_THREADS_KEY, CONCURRENT_THREADS_DESCR, 1,
				Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), true));
		params.add(new ParameterTypeInt(COST_WRONG_WRITE_KEY, "Default cost for a wrong write operation.", 0, 100, 1));
		params.add(
				new ParameterTypeInt(COST_MISSING_WRITE_KEY, "Default cost for a missing write operation.", 0, 100, 1));
		params.add(new ParameterTypeInt(COST_MODEL_MOVE_KEY, "Default cost for a model move.", 0, 100, 1));
		params.add(new ParameterTypeInt(COST_LOG_MOVE_KEY, "Default cost for a log move.", 0, 100, 1));
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

}