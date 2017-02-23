package org.rapidprom.operators.conformance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.processmining.dataawarereplayer.precision.DataAwarePrecisionPlugin;
import org.processmining.dataawarereplayer.precision.PrecisionConfig;
import org.processmining.dataawarereplayer.precision.PrecisionMeasureException;
import org.processmining.dataawarereplayer.precision.PrecisionResult;
import org.processmining.dataawarereplayer.precision.projection.ProcessProjectionException;
import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.log.utils.XUtils;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.DataElement;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.balancedconformance.BalancedDataXAlignmentPlugin;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration;
import org.processmining.plugins.balancedconformance.controlflow.ControlFlowAlignmentException;
import org.processmining.plugins.balancedconformance.dataflow.exception.DataAlignmentException;
import org.processmining.plugins.balancedconformance.observer.DataConformancePlusObserverImpl;
import org.processmining.plugins.balancedconformance.result.StatisticResult;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.processmining.xesalignmentextension.XAlignmentExtension;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignedLog;
import org.rapidprom.exceptions.ExampleSetReaderException;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.ioobjects.TransEvMappingIOObject;
import org.rapidprom.ioobjects.XAlignedLogIOObject;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.conformance.util.AlignmentCostIO;
import org.rapidprom.operators.conformance.util.DataAlignmentCostIO;
import org.rapidprom.operators.conformance.util.VariableMappingIO;
import org.rapidprom.operators.util.RapidProMProgress;

import com.google.common.base.Stopwatch;
import com.google.common.math.BigIntegerMath;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Primitives;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ProcessSetupError.Severity;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimpleMetaDataError;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;

import javassist.tools.rmi.ObjectNotFoundException;

/**
 * Implements the balanced conformance checking approach presented in the paper
 * 'Balanced multi-perspective checking of process conformance':
 * doi:10.1007/s00607-015-0441-1
 * <p/>
 * Also implements the multi-perspective precision measure presented in the
 * paper 'Measuring the Precision of Multi-perspective Process Models':
 * doi:10.1007/978-3-319-42887-1_10
 * 
 * @author F. Mannhardt
 *
 */
public class DataConformanceOperator extends Operator {

	private static final String COST_LOG_MOVE_KEY = "Cost: Log move";
	private static final String COST_MODEL_MOVE_KEY = "Cost: Model move";
	private static final String COST_MISSING_WRITE_KEY = "Cost: Missing Write";
	private static final String COST_WRONG_WRITE_KEY = "Cost: Wrong Write";

	private static final String CONCURRENT_THREADS_KEY = "Number of Threads";
	private static final String CONCURRENT_THREADS_DESCR = "Specify the number of threads used to calculate alignments in parallel."
			+ " With each extra thread, more memory is used but less time is required.";

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputModel = getInputPorts().createPort("model (ProM Data petri net)",
			DataPetriNetIOObject.class);

	private InputPort inputTransitionMapping = getInputPorts()
			.createPort("mapping (ProM Transition/Event Class Mapping)");
	private InputPort inputVariableMapping = getInputPorts().createPort("variable mapping (Example set)");

	private InputPort inputCosts = getInputPorts().createPort("costs control-flow (Example set)");
	private InputPort inputCostsData = getInputPorts().createPort("costs data (Example set)");

	private OutputPort outputAlignedLog = getOutputPorts().createPort("aligned log (ProM Aligned Event Log)");
	private OutputPort outputMetrics = getOutputPorts().createPort("fitness/precision (Example set)");
	private OutputPort outputMetricsDetailed = getOutputPorts().createPort("detailed metrics (Example set)");

	private OutputPort passthroughTransitionMapping = getOutputPorts()
			.createPort("mapping (ProM Transition/Event Class Mapping)");
	private OutputPort passthroughVariableMapping = getOutputPorts().createPort("variable mapping (Example set)");
	private OutputPort passthroughCosts = getOutputPorts().createPort("costs control-flow (Example set)");
	private OutputPort passthroughCostsData = getOutputPorts().createPort("costs data (Example set)");

	public DataConformanceOperator(OperatorDescription description) {
		super(description);
		inputTransitionMapping.addPrecondition(
				new SimplePrecondition(inputTransitionMapping, new MetaData(TransEvMappingIOObject.class), true));
		inputVariableMapping
				.addPrecondition(new SimplePrecondition(inputVariableMapping, new MetaData(ExampleSet.class), false));
		inputCosts.addPrecondition(new SimplePrecondition(inputCosts, new MetaData(ExampleSet.class), false));
		inputCostsData.addPrecondition(new SimplePrecondition(inputCostsData, new MetaData(ExampleSet.class), false));

		getTransformer().addRule(new GenerateNewMDRule(outputAlignedLog, XAlignedLogIOObject.class));

		addMetricsPort();
		addMetricsDetailedPort();

		getTransformer().addRule(new GenerateNewMDRule(passthroughTransitionMapping, TransEvMappingIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughVariableMapping, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughCosts, ExampleSet.class));
		getTransformer().addRule(new GenerateNewMDRule(passthroughCostsData, ExampleSet.class));
	}

	private void addMetricsDetailedPort() {
		getTransformer().addRule(new GenerateNewMDRule(outputMetricsDetailed, ExampleSet.class));
		ExampleSetMetaData metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("Trace", Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData("Length", Ontology.NUMERICAL);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd2);
		metaData.setNumberOfExamples(1);
		AttributeMetaData amd3 = new AttributeMetaData("Time", Ontology.NUMERICAL);
		amd3.setRole(AttributeColumn.REGULAR);
		amd3.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd3);
		metaData.setNumberOfExamples(1);
		AttributeMetaData amd4 = new AttributeMetaData("Queued States", Ontology.NUMERICAL);
		amd4.setRole(AttributeColumn.REGULAR);
		amd4.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd4);
		metaData.setNumberOfExamples(1);
		AttributeMetaData amd5 = new AttributeMetaData("Fitness", Ontology.NUMERICAL);
		amd5.setRole(AttributeColumn.REGULAR);
		amd5.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd5);
		metaData.setNumberOfExamples(1);
		getTransformer().addRule(new GenerateNewMDRule(outputMetricsDetailed, metaData));
	}

	private void addMetricsPort() {
		getTransformer().addRule(new GenerateNewMDRule(outputMetrics, ExampleSet.class));
		ExampleSetMetaData metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("Name", Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData("Value", Ontology.NUMERICAL);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd2);
		metaData.setNumberOfExamples(1);
		getTransformer().addRule(new GenerateNewMDRule(outputMetrics, metaData));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog log = getXLog();
		DataPetriNetIOObject dpnIO = inputModel.getData(DataPetriNetIOObject.class);

		try {
			TransEvClassMapping transitionMapping = getTransitionMapping();

			DataPetriNet dpn = dpnIO.getArtifact();

			Marking initialMarking = dpnIO.getInitialMarking();
			Marking[] finalMarkings = dpnIO.getFinalMarkingAsArray();

			BalancedProcessorConfiguration config = BalancedProcessorConfiguration.newDefaultInstance(dpn,
					initialMarking, finalMarkings, log, transitionMapping.getEventClassifier(), getDefaultCostLogMove(),
					getDefaultCostModelMove(), getDefaultCostMissingWrite(), getDefaultCostWrongWrite());
			applyUserDefinedTransitionMapping(transitionMapping, config);

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
					applyUserDefinedCosts(getCostsControlFlow(), log, dpn, config);
				} catch (ExampleSetReaderException e) {
					inputCosts.addError(new SimpleMetaDataError(Severity.WARNING, inputCosts, e.getMessage()));
				}
			}

			if (inputCostsData.isConnected()) {
				try {
					applyUserDefinedDataCosts(getCostsData(), log, dpn, config);
				} catch (ExampleSetReaderException e) {
					inputCostsData.addError(new SimpleMetaDataError(Severity.WARNING, inputCostsData, e.getMessage()));
				}
			}

			DataConformancePlusObserverImpl observer = new DataConformancePlusObserverImpl(dpnIO.getPluginContext());
			config.setObserver(observer);

			BalancedDataXAlignmentPlugin alignmentPlugin = new BalancedDataXAlignmentPlugin();
			Stopwatch watch = Stopwatch.createStarted();
			XLog alignLog = alignmentPlugin.alignLog(new RapidProMProgress(getProgress()), dpn, log, config);
			long runTime = watch.elapsed(TimeUnit.MILLISECONDS);
			XAlignedLog alignedLog = XAlignmentExtension.instance().extendLog(alignLog);

			PrecisionConfig precisionConfig = new PrecisionConfig(initialMarking,
					DataAwarePrecisionPlugin.convertMapping(config.getActivityMapping()),
					config.getActivityMapping().getEventClassifier(), config.getVariableMapping());
			precisionConfig.setAssumeUnassignedVariablesFree(true);
			PrecisionResult precisionResult = new DataAwarePrecisionPlugin().doMeasurePrecisionWithAlignment(dpn, log,
					alignedLog, precisionConfig);

			outputAlignedLog.deliver(new XAlignedLogIOObject(alignedLog.getLog(), dpnIO.getPluginContext()));
			outputMetrics.deliver(createMeasureTable(alignedLog, precisionResult, observer, runTime));
			outputMetricsDetailed.deliver(createMeasureDetailedTable(log, observer));

			passthroughCosts.deliver(new AlignmentCostIO().writeCostsToExampleSet(config.getMapEvClass2Cost(),
					config.getMapTrans2Cost()));
			passthroughCostsData.deliver(new DataAlignmentCostIO().writeCostsToExampleSet(config.getVariableCost()));
			passthroughVariableMapping
					.deliver(new VariableMappingIO().writeVariableMapping(config.getVariableMapping()));
			passthroughTransitionMapping.deliver(inputTransitionMapping.getData(TransEvMappingIOObject.class));

		} catch (ControlFlowAlignmentException | DataAlignmentException e) {
			throw new OperatorException("Failed alignment! Reason: " + e.getMessage(), e);
		} catch (PrecisionMeasureException | ProcessProjectionException e) {
			throw new OperatorException("Failed precision measurement " + e.getMessage(), e);
		} catch (ObjectNotFoundException e) {
			throw new OperatorException("Missing markings! Reason: " + e.getMessage(), e);
		}

	}

	private ExampleSet createMeasureDetailedTable(XLog log, DataConformancePlusObserverImpl observer) {
		Attribute traceAttr = AttributeFactory.createAttribute("Trace", Ontology.STRING);
		Attribute lengthAttr = AttributeFactory.createAttribute("Length", Ontology.NUMERICAL);
		Attribute timeAttr = AttributeFactory.createAttribute("Time", Ontology.NUMERICAL);
		Attribute queuedAttr = AttributeFactory.createAttribute("Queued States", Ontology.NUMERICAL);
		Attribute fitnessAttr = AttributeFactory.createAttribute("Fitness", Ontology.NUMERICAL);

		MemoryExampleTable table = new MemoryExampleTable(traceAttr, lengthAttr, timeAttr, queuedAttr, fitnessAttr);
		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		
		double[] timeData = observer.getStatisticResults().get(StatisticResult.TIME_PER_TRACE).getData();
		double[] queueData = observer.getStatisticResults().get(StatisticResult.QUEUED_STATES).getData();
		double[] fitnessData = observer.getStatisticResults().get(StatisticResult.FITNESS).getData();

		Iterator<XTrace> logIter = log.iterator();
		int i = 0;
		while (logIter.hasNext()) {
			XTrace trace = (XTrace) logIter.next();
			String name = "";
			if (logIter.hasNext()) {
				name = XUtils.getConceptName(logIter.next());
			}			
			table.addDataRow(factory.create(new Object[] { name, trace.size(), timeData[i], queueData[i], fitnessData[i] },
					new Attribute[] { traceAttr, lengthAttr, timeAttr, queuedAttr, fitnessAttr }));
			i++;
		}

		return table.createExampleSet();
	}

	public ExampleSet createMeasureTable(XAlignedLog alignedLog, PrecisionResult precisionResult,
			DataConformancePlusObserverImpl observer, long runTime) {
		Attribute nameAttr = AttributeFactory.createAttribute("Name", Ontology.STRING);
		Attribute valueAttr = AttributeFactory.createAttribute("Value", Ontology.NUMERICAL);
		MemoryExampleTable table = new MemoryExampleTable(nameAttr, valueAttr);
		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		table.addDataRow(factory.create(new Object[] { "averageFitness", alignedLog.getAverageFitness() },
				new Attribute[] { nameAttr, valueAttr }));
		table.addDataRow(factory.create(new Object[] { "averagePrecision", precisionResult.getPrecision() },
				new Attribute[] { nameAttr, valueAttr }));

		table.addDataRow(factory.create(new Object[] { "runTime", runTime }, new Attribute[] { nameAttr, valueAttr }));

		double cpuTime = 0.0;
		for (double timeForTrace : observer.getStatisticResults().get(StatisticResult.TIME_PER_TRACE).getData()) {
			cpuTime += timeForTrace;
		}
		table.addDataRow(factory.create(new Object[] { "cpuTime", cpuTime }, new Attribute[] { nameAttr, valueAttr }));

		table.addDataRow(
				factory.create(new Object[] { "averageQueuedStates", DoubleMath.mean(observer.getQueuedStatesArray()) },
						new Attribute[] { nameAttr, valueAttr }));

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

	private void applyUserDefinedCosts(ExampleSet controlFlowCosts, XLog log, DataPetriNet dpn,
			BalancedProcessorConfiguration alignmentConfig) throws UserError, ExampleSetReaderException {
		AlignmentCostIO costReader = new AlignmentCostIO();
		XEventClasses eventClasses = XUtils
				.createEventClasses(alignmentConfig.getActivityMapping().getEventClassifier(), log);
		Map<String, Transition> transitions = getTransitions(dpn);
		costReader.readCostsFromExampleSet(controlFlowCosts, eventClasses, transitions,
				alignmentConfig.getMapEvClass2Cost(), alignmentConfig.getMapTrans2Cost());
	}

	private void applyUserDefinedDataCosts(ExampleSet dataCosts, XLog log, DataPetriNet dpn,
			BalancedProcessorConfiguration alignmentConfig) throws UserError, ExampleSetReaderException {
		DataAlignmentCostIO costReader = new DataAlignmentCostIO();
		Set<String> variables = new HashSet<>();
		for (DataElement element : dpn.getVariables()) {
			variables.add(element.getVarName());
		}
		costReader.readCostsFromExampleSet(dataCosts, getDefaultCostWrongWrite(), getDefaultCostMissingWrite(),
				dpn.getTransitions(), variables);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
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

	private XLog getXLog() throws UserError {
		return inputXLog.getData(XLogIOObject.class).getArtifact();
	}

}