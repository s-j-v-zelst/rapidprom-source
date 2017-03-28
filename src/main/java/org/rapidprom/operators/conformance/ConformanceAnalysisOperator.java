package org.rapidprom.operators.conformance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.ArrayUtils;
import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.util.Pair;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.elements.Place;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.astar.petrinet.AbstractPetrinetReplayer;
import org.processmining.plugins.astar.petrinet.PetrinetReplayerWithILP;
import org.processmining.plugins.astar.petrinet.PetrinetReplayerWithoutILP;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.processmining.plugins.petrinet.replayer.PNLogReplayer;
import org.processmining.plugins.petrinet.replayer.algorithms.IPNReplayParameter;
import org.processmining.plugins.petrinet.replayer.algorithms.costbasedcomplete.CostBasedCompleteParam;
import org.processmining.plugins.petrinet.replayresult.PNRepResult;
import org.processmining.plugins.replayer.replayresult.SyncReplayResult;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PNRepResultIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;
import org.rapidprom.operators.util.ExecutorServiceRapidProM;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
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
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;
import com.rapidminer.parameter.conditions.EqualStringCondition;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

import javassist.tools.rmi.ObjectNotFoundException;
import nl.tue.astar.AStarException;
import nl.tue.astar.AStarThread;

public class ConformanceAnalysisOperator extends AbstractRapidProMEventLogBasedOperator {

	private class RapidProMAlignmentCallable implements Callable<PNRepResultIOObject> {

		PluginContext pluginContext;

		public RapidProMAlignmentCallable(PluginContext input) {
			pluginContext = input;
		}

		@Override
		public PNRepResultIOObject call() throws Exception {

			XLogIOObject xLog = new XLogIOObject(getXLog(), pluginContext);
			PetriNetIOObject pNet = inputPN.getData(PetriNetIOObject.class);

			PNRepResult repResult = null;
			try {
				if (!pNet.hasFinalMarking())
					pNet.setFinalMarking(getFinalMarking(pNet.getArtifact()));
				repResult = getAlignment(pluginContext, pNet.getArtifact(), xLog.getArtifact(),
						pNet.getInitialMarking(), pNet.getFinalMarking());
			} catch (ObjectNotFoundException e1) {
				e1.printStackTrace();
			}

			PNRepResultIOObject result = new PNRepResultIOObject(repResult, pluginContext, pNet, xLog.getArtifact(),
					constructMapping(pNet.getArtifact(), xLog.getArtifact(), getXEventClassifier()));

			return result;
		}

	}

	private static final String PARAMETER_DESC_DEP_ILP_USE_FAST_LOWER_BOUNDS = "Use result of previous ILP as an estimator for the heuristic";
	private static final String PARAMETER_DESC_DEP_ILP_USE_INTEGERS = "Restrict LP variables to be integers, i.e. ILP";

	private static final String PARAMETER_DESC_HEURISTIC_FUNCTION_TYPE = "Select what heuristic function to use in the A* search, plain is the regular A* heuristic, weighted implies that the heuristic of a vertex v is multiplied with (1 + eps), i.e. h'(v) = (1 + eps)h(v), dynamic weighting decreases the impact of eps if we are closer to the end goal.";
	private static final String PARAMETER_DESC_MAX_STATES = "The maximum number of states that are searched for a trace alignment.";

	private static final String PARAMETER_DESC_NON_SYNCHRONOUS_MOVE_SORTING = "Enumeration to set the sorting of moves. When computing an alignment, it is generally possible to sort the moves in between two synchronous moves.  If LOGMOVEFIST is used, then no log-move will ever succeed a model-Move. If MOVEMODELFIRST is used, then no model-move will ever succeed alog-move.";
	private static final String PARAMETER_DESC_NUM_THREADS = "Specify the number of threads used to calculate alignments in parallel."
			+ " With each extra thread, more memory is used but less cpu time is required.";

	private static final String PARAMETER_DESC_QUEUEING_MODEL = "Defines how states in queue will be traversed, BFS, DFS or random.";
	private static final String PARAMETER_DESC_TIME_OUT = "The number of seconds that this operator will run before "
			+ "returning whatever it could manage to calculate (or null otherwise).";
	private static final String PARAMETER_DESC_USE_ILP = "Whether or not we use the marking equation (ILP) to compute a heuristic for the A* search.";
	private static final String PARAMETER_DESC_WEIGHTED_EPSILON = "Give a value for epsilon used in weighted heuristic";
	private static final String PARAMETER_KEY_DEP_ILP_USE_FAST_LOWER_BOUNDS = "Use fast lower bounds";
	private static final String PARAMETER_KEY_DEP_ILP_USE_INTEGER = "Restrict to integers";
	private static final String PARAMETER_KEY_HEURISTIC_FUNCTION_TYPE = "Heuristic Function";
	private static final String PARAMETER_KEY_MAX_STATES = "Max Explored States (in Thousands)";

	private static final String PARAMETER_KEY_NON_SYNCHRONOUS_MOVE_SORTING = "Non-synchronous move sorting";
	private static final String PARAMETER_KEY_NUM_THREADS = "Number of Threads";

	private static final String PARAMETER_KEY_QUEUEING_MODEL = "Queueing Model";
	private static final String PARAMETER_KEY_TIME_OUT = "Timeout (sec)";

	private static final String PARAMETER_KEY_USE_ILP = "Use ILP for heuristic";
	private static final String PARAMETER_KEY_WEIGHTED_EPSILON = "Epsilon";

	private static TransEvClassMapping constructMapping(PetrinetGraph net, XLog log, XEventClassifier eventClassifier) {
		TransEvClassMapping mapping = new TransEvClassMapping(eventClassifier, new XEventClass("DUMMY", 99999));

		XLogInfo summary = XLogInfoFactory.createLogInfo(log, eventClassifier);

		for (Transition t : net.getTransitions()) {
			for (XEventClass evClass : summary.getEventClasses().getClasses()) {
				String id = evClass.getId();

				if (t.getLabel().equals(id)) {
					mapping.put(t, evClass);
					break;
				}
			}

		}

		return mapping;
	}

	private static Map<Transition, Integer> constructMOSCostFunction(PetrinetGraph net) {
		Map<Transition, Integer> costMOS = new HashMap<Transition, Integer>();

		for (Transition t : net.getTransitions())
			if (t.isInvisible())
				costMOS.put(t, 0);
			else
				costMOS.put(t, 1);

		return costMOS;
	}

	private static Map<XEventClass, Integer> constructMOTCostFunction(PetrinetGraph net, XLog log,
			XEventClassifier eventClassifier) {
		Map<XEventClass, Integer> costMOT = new HashMap<XEventClass, Integer>();
		XLogInfo summary = XLogInfoFactory.createLogInfo(log, eventClassifier);

		for (XEventClass evClass : summary.getEventClasses().getClasses()) {
			costMOT.put(evClass, 1);
		}

		return costMOT;
	}

	@SuppressWarnings("rawtypes")
	public static Marking getFinalMarking(Petrinet pn) {
		List<Place> places = new ArrayList<Place>();
		Iterator<Place> placesIt = pn.getPlaces().iterator();
		while (placesIt.hasNext()) {
			Place nextPlace = placesIt.next();
			Collection inEdges = pn.getOutEdges(nextPlace);
			if (inEdges.isEmpty()) {
				places.add(nextPlace);
			}
		}
		Marking finalMarking = new Marking();
		for (Place place : places) {
			finalMarking.add(place);
		}
		return finalMarking;
	}

	private PNRepResultIOObject alignments = null;

	private final String COLUMN_LL_ALIGNMENT_COST = "absolute_alignment_cost";
	private final String COLUMN_LL_AVG_VISITED_STATES = "visited_states";
	private final String COLUMN_LL_COMPUTATION_TIME = "computation_time";
	private final String COLUMN_LL_MOVE_LOG_FITNESS = "move_log_fitness";
	private final String COLUMN_LL_MOVE_MODEL_FITNESS = "move_model_fitness";
	private final String COLUMN_LL_QUEUED_STATES = "queued_states";
	private final String COLUMN_LL_RELIABLE = "reliable";
	private final String COLUMN_LL_TRACE_FITNESS = "trace_fitness";

	private final String COLUMN_TL_TRACE_ID = "trace_identifier";
	private final String COLUMN_TL_TRACE_INDEX = "trace_index";
	private final String COLUMN_TVL_COMPUTATION_TIME = "computation_time";
	private final String COLUMN_TVL_NUM_STATES_VISITED = "visited_states";
	private final String COLUMN_TVL_NUM_TRACES = "number_of_traces";
	private final String COLUMN_TVL_QUEUED_STATES = "queued_states";
	private final String COLUMN_TVL_TL_ALIGNMENT_COSTS = "alignment_cost";
	private final String COLUMN_TVL_TL_MOVE_LOG_FITNESS = "move_log_fitness";
	private final String COLUMN_TVL_TL_MOVE_MODEL_FITNESS = "move_model_fitness";
	private final String COLUMN_TVL_TL_RELIABLE = "reliable";
	private final String COLUMN_TVL_TL_TRACE_FITNESS = "trace_fitness";
	private final String COLUMN_TVL_TRACE_INDICES = "trace_indicess";

	private final int[] COLUMN_TYPES_LOG_LEVEL = new int[] { Ontology.BINOMINAL, Ontology.REAL, Ontology.REAL,
			Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.REAL };

	private final int[] COLUMN_TYPES_TRACE_LEVEL = new int[] { Ontology.INTEGER, Ontology.STRING, Ontology.BINOMINAL,
			Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.INTEGER };

	private final int[] COLUMN_TYPES_TRACE_VARIANT_LEVEL = new int[] { Ontology.STRING, Ontology.INTEGER,
			Ontology.BINOMINAL, Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.INTEGER, Ontology.INTEGER,
			Ontology.INTEGER, Ontology.REAL };

	private final MDInteger[] COLUMNS_MISSING_LOG_LEVEL = new MDInteger[] { new MDInteger(0), new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0),
			new MDInteger(0) };

	private final MDInteger[] COLUMNS_MISSING_TRACE_LEVEL = new MDInteger[] { new MDInteger(0), new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0),
			new MDInteger(0) };

	private final MDInteger[] COLUMNS_MISSING_TRACE_VARIANT_LEVEL = new MDInteger[] { new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0) };

	private final String[] COLUMNS_NAMES_LOG_LEVEL = new String[] { COLUMN_LL_RELIABLE, COLUMN_LL_TRACE_FITNESS,
			COLUMN_LL_MOVE_LOG_FITNESS, COLUMN_LL_MOVE_MODEL_FITNESS, COLUMN_LL_ALIGNMENT_COST,
			COLUMN_LL_AVG_VISITED_STATES, COLUMN_LL_QUEUED_STATES, COLUMN_LL_COMPUTATION_TIME };

	private final String[] COLUMNS_NAMES_TRACE_LEVEL = new String[] { COLUMN_TL_TRACE_INDEX, COLUMN_TL_TRACE_ID,
			COLUMN_TVL_TL_RELIABLE, COLUMN_TVL_TL_TRACE_FITNESS, COLUMN_TVL_TL_MOVE_LOG_FITNESS,
			COLUMN_TVL_TL_MOVE_MODEL_FITNESS, COLUMN_TVL_TL_ALIGNMENT_COSTS };

	private final String[] COLUMNS_NAMES_TRACE_VARIANT_LEVEL = new String[] { COLUMN_TVL_TRACE_INDICES,
			COLUMN_TVL_NUM_TRACES, COLUMN_TVL_TL_RELIABLE, COLUMN_TVL_TL_TRACE_FITNESS, COLUMN_TVL_TL_MOVE_LOG_FITNESS,
			COLUMN_TVL_TL_MOVE_MODEL_FITNESS, COLUMN_TVL_TL_ALIGNMENT_COSTS, COLUMN_TVL_NUM_STATES_VISITED,
			COLUMN_TVL_QUEUED_STATES, COLUMN_TVL_COMPUTATION_TIME };

	private final String[] COLUMNS_ROLES_LOG_LEVEL = new String[] { AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR };

	private final String[] COLUMNS_ROLES_TRACE_LEVEL = new String[] { AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR };

	private final String[] COLUMNS_ROLES_TRACE_VARIANT_LEVEL = new String[] { AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR };

	private final DataRowFactory dataRowFactory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
	private InputPort inputPN = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);

	private OutputPort outputPortLogLevelStatistics = getOutputPorts()
			.createPort("example set with metrics, log level (Data Table)");
	private ExampleSetMetaData logLevelStatisticsMetaData = null;

	private OutputPort outputPortTraceVariantLevelStatistics = getOutputPorts()
			.createPort("example set with alignment values, trace-variant level (Data Table)");
	private ExampleSetMetaData traceVariantLevelStatisticsMetaData = null;

	private OutputPort outputPortTraceLevelStatistics = getOutputPorts()
			.createPort("example set with alignment values, trace level (Data Table)");

	private ExampleSetMetaData traceLevelStatisticsMetaData = null;

	private OutputPort outputPortProMObject = getOutputPorts().createPort("alignments (ProM PNRepResult)");

	public ConformanceAnalysisOperator(OperatorDescription description) {
		super(description);

		logLevelStatisticsMetaData = constructExampleSetMetaData(new ExampleSetMetaData(), COLUMNS_NAMES_LOG_LEVEL,
				COLUMN_TYPES_LOG_LEVEL, COLUMNS_ROLES_LOG_LEVEL, COLUMNS_MISSING_LOG_LEVEL);
		getTransformer().addRule(new GenerateNewMDRule(outputPortLogLevelStatistics, logLevelStatisticsMetaData));

		traceVariantLevelStatisticsMetaData = constructExampleSetMetaData(new ExampleSetMetaData(),
				COLUMNS_NAMES_TRACE_VARIANT_LEVEL, COLUMN_TYPES_TRACE_VARIANT_LEVEL, COLUMNS_ROLES_TRACE_VARIANT_LEVEL,
				COLUMNS_MISSING_TRACE_VARIANT_LEVEL);
		getTransformer().addRule(
				new GenerateNewMDRule(outputPortTraceVariantLevelStatistics, traceVariantLevelStatisticsMetaData));

		traceLevelStatisticsMetaData = constructExampleSetMetaData(new ExampleSetMetaData(), COLUMNS_NAMES_TRACE_LEVEL,
				COLUMN_TYPES_TRACE_LEVEL, COLUMNS_ROLES_TRACE_LEVEL, COLUMNS_MISSING_TRACE_LEVEL);
		getTransformer().addRule(new GenerateNewMDRule(outputPortTraceLevelStatistics, traceLevelStatisticsMetaData));

		getTransformer().addRule(new GenerateNewMDRule(outputPortProMObject, PNRepResultIOObject.class));
	}

	private ExampleSetMetaData constructExampleSetMetaData(final ExampleSetMetaData metaData, final String[] names,
			final int[] types, final String[] roles, final MDInteger[] missing) {
		for (int i = 0; i < names.length; i++) {
			AttributeMetaData amd = new AttributeMetaData(names[i], types[i]);
			amd.setRole(roles[i]);
			amd.setNumberOfMissingValues(missing[i]);
			metaData.addAttribute(amd);
		}
		return metaData;
	}

	public ExampleSet constructLogLevelStatistics(final PNRepResult repResult, final boolean reliable) {
		Attribute[] attributes = new Attribute[COLUMNS_NAMES_LOG_LEVEL.length];
		for (int i = 0; i < COLUMNS_NAMES_LOG_LEVEL.length; i++) {
			attributes[i] = AttributeFactory.createAttribute(COLUMNS_NAMES_LOG_LEVEL[i], COLUMN_TYPES_LOG_LEVEL[i]);
		}
		Object[] values = new Object[COLUMNS_NAMES_LOG_LEVEL.length];
		Map<String, Object> info = reliable ? repResult.getInfo() : null;

		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_RELIABLE)] = reliable
				? (new Boolean(true)).toString() : (new Boolean(false)).toString();
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_TRACE_FITNESS)] = reliable
				? (Double) info.get(PNRepResult.TRACEFITNESS) : Double.NaN;
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_MOVE_LOG_FITNESS)] = reliable
				? (Double) info.get(PNRepResult.MOVELOGFITNESS) : Double.NaN;
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_MOVE_MODEL_FITNESS)] = reliable
				? (Double) info.get(PNRepResult.MOVEMODELFITNESS) : Double.NaN;
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_ALIGNMENT_COST)] = reliable
				? (Double) info.get(PNRepResult.RAWFITNESSCOST) : Double.NaN;
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_AVG_VISITED_STATES)] = reliable
				? (Double) info.get(PNRepResult.NUMSTATEGENERATED) : Double.NaN;
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_QUEUED_STATES)] = reliable
				? (Double) info.get(PNRepResult.QUEUEDSTATE) : Double.NaN;
		values[ArrayUtils.indexOf(COLUMNS_NAMES_LOG_LEVEL, COLUMN_LL_COMPUTATION_TIME)] = reliable
				? (Double) info.get(PNRepResult.TIME) : Double.NaN;
		MemoryExampleTable table = new MemoryExampleTable(attributes);
		table.addDataRow(getDataRowFactory().create(values, attributes));
		return table.createExampleSet();
	}

	public Pair<ExampleSet, ExampleSet> constructTraceLevelStatistics(PNRepResult repResult) throws UserError {
		Attribute[] traceVariantAttributes = new Attribute[COLUMNS_NAMES_TRACE_VARIANT_LEVEL.length];
		for (int i = 0; i < traceVariantAttributes.length; i++) {
			traceVariantAttributes[i] = AttributeFactory.createAttribute(COLUMNS_NAMES_TRACE_VARIANT_LEVEL[i],
					COLUMN_TYPES_TRACE_VARIANT_LEVEL[i]);
		}
		MemoryExampleTable traceVariantTable = new MemoryExampleTable(traceVariantAttributes);
		Attribute[] traceAttributes = new Attribute[COLUMNS_NAMES_TRACE_LEVEL.length];
		for (int i = 0; i < traceAttributes.length; i++) {
			traceAttributes[i] = AttributeFactory.createAttribute(COLUMNS_NAMES_TRACE_LEVEL[i],
					COLUMN_TYPES_TRACE_LEVEL[i]);
		}
		MemoryExampleTable traceTable = new MemoryExampleTable(traceAttributes);
		for (SyncReplayResult res : repResult) {
			Object[] traceVariantValues = new Object[traceVariantAttributes.length];
			Object[] traceValues = new Object[traceAttributes.length];
			// alignment costs
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL,
					COLUMN_TVL_TL_ALIGNMENT_COSTS)] = traceValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL,
							COLUMN_TVL_TL_ALIGNMENT_COSTS)] = res.getInfo().get(PNRepResult.RAWFITNESSCOST);

			// move log
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL,
					COLUMN_TVL_TL_MOVE_LOG_FITNESS)] = traceValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL,
							COLUMN_TVL_TL_MOVE_LOG_FITNESS)] = res.getInfo().get(PNRepResult.MOVELOGFITNESS);

			// move model
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL,
					COLUMN_TVL_TL_MOVE_MODEL_FITNESS)] = traceValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL,
							COLUMN_TVL_TL_MOVE_MODEL_FITNESS)] = res.getInfo().get(PNRepResult.MOVEMODELFITNESS);

			// reliable
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL,
					COLUMN_TVL_TL_RELIABLE)] = traceValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL,
							COLUMN_TVL_TL_RELIABLE)] = res.isReliable() ? (new Boolean(true)).toString()
									: (new Boolean(false)).toString();

			// trace fitness
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL,
					COLUMN_TVL_TL_TRACE_FITNESS)] = traceValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL,
							COLUMN_TVL_TL_TRACE_FITNESS)] = res.getInfo().get(PNRepResult.TRACEFITNESS);

			// time
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL, COLUMN_TVL_COMPUTATION_TIME)] = res
					.getInfo().get(PNRepResult.TIME);

			// states
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL,
					COLUMN_TVL_NUM_STATES_VISITED)] = res.getInfo().get(PNRepResult.NUMSTATEGENERATED);

			// queued
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL, COLUMN_TVL_QUEUED_STATES)] = res
					.getInfo().get(PNRepResult.QUEUEDSTATE);

			List<Integer> listArray = convertIntListToArray(res.getTraceIndex().toString());
			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL, COLUMN_TVL_TRACE_INDICES)] = res
					.getTraceIndex().toString();

			traceVariantValues[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_VARIANT_LEVEL, COLUMN_TVL_NUM_TRACES)] = listArray
					.size();

			traceVariantTable.addDataRow(getDataRowFactory().create(traceVariantValues, traceVariantAttributes));

			for (Integer index : listArray) {
				// get the right trace
				Object[] traceInstance = Arrays.copyOf(traceValues, traceValues.length);
				traceInstance[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL, COLUMN_TL_TRACE_INDEX)] = index;
				traceInstance[ArrayUtils.indexOf(COLUMNS_NAMES_TRACE_LEVEL, COLUMN_TL_TRACE_ID)] = XConceptExtension
						.instance().extractName(getXLog().get(index));
				traceTable.addDataRow(getDataRowFactory().create(traceInstance, traceAttributes));
			}
		}
		return new Pair<ExampleSet, ExampleSet>(traceVariantTable.createExampleSet(), traceTable.createExampleSet());

	}

	private boolean containsUnreliableAlignment(final PNRepResult repRes) {
		for (SyncReplayResult r : repRes) {
			if (!r.isReliable()) {
				return true;
			}
		}
		return false;

	}

	private List<Integer> convertIntListToArray(String s) {
		List<Integer> result = new ArrayList<Integer>();
		s = s.replace("[", "");
		s = s.replace("]", "");
		String[] split = s.split(",");
		for (int i = 0; i < split.length; i++) {
			String string = split[i];
			String trim = string.trim();
			Integer in = Integer.parseInt(trim);
			result.add(in);
		}
		return result;
	}

	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: replay log on petri net for conformance checking");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance()
				.getFutureResultAwarePluginContext(PNLogReplayer.class);
		SimpleTimeLimiter limiter = new SimpleTimeLimiter(new ExecutorServiceRapidProM(pluginContext));

		PNRepResult repResult = null;

		try {
			alignments = limiter.callWithTimeout(new RapidProMAlignmentCallable(pluginContext),
					getParameterAsInt(PARAMETER_KEY_TIME_OUT), TimeUnit.SECONDS, true);
			repResult = alignments.getArtifact();

			outputPortProMObject.deliver(alignments);

		} catch (Exception e) {
			logger.log(Level.INFO, "Conformance Checker timed out.");
			outputPortProMObject.deliver(new PNRepResultIOObject(null, pluginContext, null, null, null));
		}

		repResult = repResult.isEmpty() ? null : repResult;
		final boolean reliable = repResult == null ? false : !containsUnreliableAlignment(repResult);
		outputPortLogLevelStatistics.deliver(constructLogLevelStatistics(repResult, reliable));
		Pair<ExampleSet, ExampleSet> traceStatistics = constructTraceLevelStatistics(repResult);
		outputPortTraceVariantLevelStatistics.deliver(traceStatistics.getFirst());
		outputPortTraceLevelStatistics.deliver(traceStatistics.getSecond());

		logger.log(Level.INFO, "End: replay log on petri net for conformance checking ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public PNRepResult getAlignment(PluginContext pluginContext, PetrinetGraph net, XLog log, Marking initialMarking,
			Marking finalMarking) throws UserError {

		Map<Transition, Integer> costMOS = constructMOSCostFunction(net);
		XEventClassifier eventClassifier = getXEventClassifier();
		Map<XEventClass, Integer> costMOT = constructMOTCostFunction(net, log, eventClassifier);
		TransEvClassMapping mapping = constructMapping(net, log, eventClassifier);

		AbstractPetrinetReplayer<?, ?> replayEngine = null;
		if (getParameterAsBoolean(PARAMETER_KEY_USE_ILP)) {
			replayEngine = new PetrinetReplayerWithILP(getParameterAsBoolean(PARAMETER_KEY_DEP_ILP_USE_INTEGER),
					getParameterAsBoolean(PARAMETER_KEY_DEP_ILP_USE_FAST_LOWER_BOUNDS));
		} else {
			replayEngine = new PetrinetReplayerWithoutILP();
		}

		IPNReplayParameter parameters = new CostBasedCompleteParam(costMOT, costMOS);
		parameters.setInitialMarking(initialMarking);
		parameters.setFinalMarkings(finalMarking);
		parameters.setGUIMode(false);
		parameters.setCreateConn(false);
		parameters.setNumThreads(getParameterAsInt(PARAMETER_KEY_NUM_THREADS));
		parameters.setQueueingModel(getUIQueueingModel());
		parameters.setAsynchronousMoveSort(getUIASyncMoveSorting());
		parameters.setType(getUIHeuristicType());

		((CostBasedCompleteParam) parameters).setMaxNumOfStates(getParameterAsInt(PARAMETER_KEY_MAX_STATES) * 1000);

		PNRepResult result = null;
		try {
			result = replayEngine.replayLog(pluginContext, net, log, mapping, parameters);

		} catch (AStarException e) {
			e.printStackTrace();
		}

		return result;
	}

	public DataRowFactory getDataRowFactory() {
		return dataRowFactory;
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeInt paramMaxStates = new ParameterTypeInt(PARAMETER_KEY_MAX_STATES, PARAMETER_DESC_MAX_STATES, 0,
				Integer.MAX_VALUE, 200, false);
		parameterTypes.add(paramMaxStates);

		ParameterTypeInt paramTimeOut = new ParameterTypeInt(PARAMETER_KEY_TIME_OUT, PARAMETER_DESC_TIME_OUT, 0,
				Integer.MAX_VALUE, 60, false);
		parameterTypes.add(paramTimeOut);

		// keep one core available to not completely drain the cpu
		ParameterTypeInt paramNumThreads = new ParameterTypeInt(PARAMETER_KEY_NUM_THREADS, PARAMETER_DESC_NUM_THREADS,
				1, Integer.MAX_VALUE, Runtime.getRuntime().availableProcessors() - 1, false);
		parameterTypes.add(paramNumThreads);

		ParameterTypeBoolean paramILP = new ParameterTypeBoolean(PARAMETER_KEY_USE_ILP, PARAMETER_DESC_USE_ILP, true,
				true);
		parameterTypes.add(paramILP);

		ParameterTypeBoolean paramRestrictILPToInts = new ParameterTypeBoolean(PARAMETER_KEY_DEP_ILP_USE_INTEGER,
				PARAMETER_DESC_DEP_ILP_USE_INTEGERS, true, true);
		paramRestrictILPToInts.setOptional(true);
		paramRestrictILPToInts
				.registerDependencyCondition(new BooleanParameterCondition(this, PARAMETER_KEY_USE_ILP, true, true));
		parameterTypes.add(paramRestrictILPToInts);

		ParameterTypeBoolean paramILPFastLoBo = new ParameterTypeBoolean(PARAMETER_DESC_DEP_ILP_USE_FAST_LOWER_BOUNDS,
				PARAMETER_DESC_DEP_ILP_USE_FAST_LOWER_BOUNDS, true, true);
		paramILPFastLoBo.setOptional(true);
		paramILPFastLoBo
				.registerDependencyCondition(new BooleanParameterCondition(this, PARAMETER_KEY_USE_ILP, true, true));
		parameterTypes.add(paramILPFastLoBo);

		String[] heuristicFunctionCategories = new String[AStarThread.Type.values().length];
		int i = 0;
		for (AStarThread.Type t : AStarThread.Type.values()) {
			heuristicFunctionCategories[i] = t.toString();
			i++;
		}
		ParameterTypeCategory paramHeuristicFunctionVariation = new ParameterTypeCategory(
				PARAMETER_KEY_HEURISTIC_FUNCTION_TYPE, PARAMETER_DESC_HEURISTIC_FUNCTION_TYPE,
				heuristicFunctionCategories, 0, true);
		parameterTypes.add(paramHeuristicFunctionVariation);

		ParameterTypeDouble paramHeuristicEpsilon = new ParameterTypeDouble(PARAMETER_KEY_WEIGHTED_EPSILON,
				PARAMETER_DESC_WEIGHTED_EPSILON, 0, Double.MAX_VALUE, 0, true);
		paramHeuristicEpsilon.setOptional(true);
		paramHeuristicEpsilon
				.registerDependencyCondition(new EqualStringCondition(this, PARAMETER_KEY_HEURISTIC_FUNCTION_TYPE, true,
						AStarThread.Type.WEIGHTED_DYNAMIC.toString(), AStarThread.Type.WEIGHTED_STATIC.toString()));
		parameterTypes.add(paramHeuristicEpsilon);

		String[] queueCats = new String[AStarThread.QueueingModel.values().length];
		i = 0;
		for (AStarThread.QueueingModel q : AStarThread.QueueingModel.values()) {
			queueCats[i] = q.toString();
			i++;
		}
		ParameterTypeCategory paramQueue = new ParameterTypeCategory(PARAMETER_KEY_QUEUEING_MODEL,
				PARAMETER_DESC_QUEUEING_MODEL, queueCats, 0, true);
		parameterTypes.add(paramQueue);

		String[] nSyncSort = new String[AStarThread.ASynchronousMoveSorting.values().length];
		i = 0;
		for (AStarThread.ASynchronousMoveSorting a : AStarThread.ASynchronousMoveSorting.values()) {
			nSyncSort[i] = a.toString();
			i++;
		}
		ParameterTypeCategory paramNonSyncSort = new ParameterTypeCategory(PARAMETER_KEY_NON_SYNCHRONOUS_MOVE_SORTING,
				PARAMETER_DESC_NON_SYNCHRONOUS_MOVE_SORTING, nSyncSort, 0, true);
		parameterTypes.add(paramNonSyncSort);

		return parameterTypes;
	}

	public AStarThread.ASynchronousMoveSorting getUIASyncMoveSorting() throws UndefinedParameterError {
		String str = getParameterAsString(PARAMETER_KEY_NON_SYNCHRONOUS_MOVE_SORTING);
		for (AStarThread.ASynchronousMoveSorting sort : AStarThread.ASynchronousMoveSorting.values()) {
			if (sort.toString().equals(str)) {
				return sort;
			}
		}
		return null;
	}

	public AStarThread.Type getUIHeuristicType() throws UndefinedParameterError {
		String str = getParameterAsString(PARAMETER_KEY_HEURISTIC_FUNCTION_TYPE);
		for (AStarThread.Type type : AStarThread.Type.values()) {
			if (type.toString().equals(str)) {
				return type;
			}
		}
		return null;
	}

	public AStarThread.QueueingModel getUIQueueingModel() throws UndefinedParameterError {
		String str = getParameterAsString(PARAMETER_KEY_QUEUEING_MODEL);
		for (AStarThread.QueueingModel qm : AStarThread.QueueingModel.values()) {
			if (qm.toString().equals(str)) {
				return qm;
			}
		}
		return null;
	}
}
