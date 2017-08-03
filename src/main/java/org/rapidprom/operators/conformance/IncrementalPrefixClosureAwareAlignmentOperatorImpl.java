package org.rapidprom.operators.conformance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.onlineconformance.algorithms.IncrementalReplayer;
import org.processmining.onlineconformance.models.IncrementalReplayResult;
import org.processmining.onlineconformance.models.MeasurementAwarePartialAlignment;
import org.processmining.onlineconformance.models.ModelSemanticsPetrinet;
import org.processmining.onlineconformance.models.PartialAlignment;
import org.processmining.onlineconformance.parameters.IncrementalRevBasedReplayerParametersImpl;
import org.processmining.plugins.petrinet.replayresult.PNRepResult;
import org.processmining.plugins.replayer.replayresult.SyncReplayResult;
import org.rapidprom.ioobjects.PNRepResultIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;
import org.rapidprom.operators.logmanipulation.PrefixClosureOperatorImpl;
import org.rapidprom.util.ExampleSetUtils;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import javassist.tools.rmi.ObjectNotFoundException;

// specific operator for additional experiments for incremental alignment paper.
public class IncrementalPrefixClosureAwareAlignmentOperatorImpl extends AbstractRapidProMEventLogBasedOperator {

	private ExampleSetMetaData alignmentsMetaData = null;

	// some columns not used for paper experiment efficiency!
	private final String COLUMN_NAME_AVG_QUEUE_SIZE = "average_queue_size";
	private final String COLUMN_NAME_PREFIX_ALIGNMENT_COST = "prefix_alignment_cost";
	private final String COLUMN_NAME_CONVENTIONAL_ALIGNMENT_COST = "conv_alignment_cost";
	private final String COLUMN_NAME_COST_DELTA_OPTIMAL_PREFIX_ALIGNMENT = "cost_delta_optimal_prefix";
	private final String COLUMN_NAME_COST_DELTA_EVENTUAL_FULL_ALIGNMENT = "cost_delta_full_alignment";
	private final String COLUMN_NAME_COST_DELTA_CONVENTIONAL_EVENTUAL_FULL_ALIGNMENT = "cost_delta_conv_vs_full_alignment";

	private final String COLUMN_NAME_ENQUEUED_NODES = "enqueued_nodes";
	// private final String COLUMN_NAME_PREFIX = "prefix";
	private final String COLUMN_NAME_PREFIX_LENGTH = "prefix_length";
	// private final String COLUMN_NAME_PREFIX_ALIGNMENT = "prefix_alignment";
	private final String COLUMN_NAME_SEARCH_TIME = "search_time";
	private final String COLUMN_NAME_TRACE = "trace_id";
	// private final String COLUMN_NAME_TRACE_FREQ = "trace_freq";
	private final String COLUMN_NAME_VISITED_NODES = "visited_nodes";
	private final String COLUMN_NAME_TRAVERSED_EDGES = "traversed_edges";

	private final MDInteger[] COLUMNS_MISSING_ALIGNMENT_TABLE = new MDInteger[] { new MDInteger(0), new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0) };

	private final String[] COLUMNS_NAMES_ALIGNMENT_TABLE = new String[] { COLUMN_NAME_TRACE, COLUMN_NAME_PREFIX_LENGTH,
			COLUMN_NAME_PREFIX_ALIGNMENT_COST, COLUMN_NAME_CONVENTIONAL_ALIGNMENT_COST,
			COLUMN_NAME_COST_DELTA_OPTIMAL_PREFIX_ALIGNMENT, COLUMN_NAME_COST_DELTA_EVENTUAL_FULL_ALIGNMENT,
			COLUMN_NAME_COST_DELTA_CONVENTIONAL_EVENTUAL_FULL_ALIGNMENT, COLUMN_NAME_ENQUEUED_NODES,
			COLUMN_NAME_VISITED_NODES, COLUMN_NAME_TRAVERSED_EDGES, COLUMN_NAME_AVG_QUEUE_SIZE,
			COLUMN_NAME_SEARCH_TIME };

	private final String[] COLUMNS_ROLES_ALIGNMENT_TABLE = new String[] { AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR };

	private final int[] COLUMNS_TYPES_ALIGNMENT_TABLE = new int[] { Ontology.STRING, Ontology.INTEGER, Ontology.REAL,
			Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.REAL, Ontology.INTEGER, Ontology.INTEGER,
			Ontology.INTEGER, Ontology.REAL, Ontology.INTEGER };

	private final DataRowFactory dataRowFactory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

	private InputPort inputPN = getInputPorts().createPort("model (Petri Net)", PetriNetIOObject.class);

	private InputPort inputPNRepRes = getInputPorts().createPort("pnrepres", PNRepResultIOObject.class);

	private OutputPort outputAlignments = getOutputPorts().createPort("example set with alignments");
	private final String PARAMETER_DESC_MAX_LOOKBACK = "Indicates the maximum amount of alignment moves that are allowed to be reverted.";

	private final String PARAMETER_DESC_UPBO = "Indicates whether or not we want to use the \"trivial\" label-based move as an upperbound for the expected prefix-alignment costs.";
	private final String PARAMETER_KEY_MAX_LOOKBACK = "Maximum Lookback";

	private final String PARAMETER_KEY_UPBO = "Use Alignment Upperbounds";

	private final Map<String, Double> fullAlignmentCosts = new HashMap<>();

	public IncrementalPrefixClosureAwareAlignmentOperatorImpl(OperatorDescription description) {
		super(description);
		alignmentsMetaData = ExampleSetUtils.constructExampleSetMetaData(new ExampleSetMetaData(),
				COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMNS_TYPES_ALIGNMENT_TABLE, COLUMNS_ROLES_ALIGNMENT_TABLE,
				COLUMNS_MISSING_ALIGNMENT_TABLE);
		getTransformer().addRule(new GenerateNewMDRule(outputAlignments, alignmentsMetaData));
	}

	private ExampleSet constructExampleSet(
			final IncrementalReplayResult<String, String, Transition, Marking, MeasurementAwarePartialAlignment<String, Transition, Marking>> replayResult,
			final TObjectIntMap<String> traceCount, Map<String, Double> fullAlignmentsCosts,
			final Map<String, List<Double>> conventionalCostsPerPrefix) {

		Attribute[] attributes = new Attribute[COLUMNS_NAMES_ALIGNMENT_TABLE.length];
		for (int i = 0; i < attributes.length; i++) {
			attributes[i] = AttributeFactory.createAttribute(COLUMNS_NAMES_ALIGNMENT_TABLE[i],
					COLUMNS_TYPES_ALIGNMENT_TABLE[i]);
		}
		ExampleSetBuilder tableBuilder = ExampleSets.from(attributes);

		for (String trace : replayResult.keySet()) {
			double fullAlignmentCost = fullAlignmentsCosts.get(trace);
			int prefixLength = 0;
			for (MeasurementAwarePartialAlignment<String, Transition, Marking> alignment : replayResult.get(trace)) {
				Object[] row = new Object[attributes.length];
				// row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
				// COLUMN_NAME_TRACE)] = trace;
				// row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
				// COLUMN_NAME_TRACE_FREQ)] = traceCount.get(trace);
				// row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
				// COLUMN_NAME_PREFIX)] = StringUtils
				// .join(alignment.projectOnLabels(), ",");
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_TRACE)] = trace;
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_PREFIX_LENGTH)] = alignment
						.projectOnLabels().size();
				// row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
				// COLUMN_NAME_PREFIX_ALIGNMENT)] = alignment
				// .toString();
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_PREFIX_ALIGNMENT_COST)] = alignment
						.getCost();
				double convCost = conventionalCostsPerPrefix.get(trace).get(prefixLength);
				prefixLength++;
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
						COLUMN_NAME_CONVENTIONAL_ALIGNMENT_COST)] = convCost;
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
						COLUMN_NAME_COST_DELTA_OPTIMAL_PREFIX_ALIGNMENT)] = alignment.getDistanceToOptimum();
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
						COLUMN_NAME_COST_DELTA_EVENTUAL_FULL_ALIGNMENT)] = alignment.getCost() - fullAlignmentCost;
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE,
						COLUMN_NAME_COST_DELTA_CONVENTIONAL_EVENTUAL_FULL_ALIGNMENT)] = convCost - fullAlignmentCost;
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_ENQUEUED_NODES)] = (int) alignment
						.getTotalEnqueuedNodes();
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_VISITED_NODES)] = alignment
						.getNumberOfIterations();
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_TRAVERSED_EDGES)] = alignment
						.getTraversedEdges();
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_AVG_QUEUE_SIZE)] = alignment
						.getAverageQueueSize();
				row[ArrayUtils.indexOf(COLUMNS_NAMES_ALIGNMENT_TABLE, COLUMN_NAME_SEARCH_TIME)] = alignment
						.getComputationTime();
				tableBuilder.addDataRow(getDataRowFactory().create(row, attributes));
			}
		}
		return tableBuilder.build();
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

	private Map<String, Double> constructFullAlignmentsReference(PNRepResult repRes) throws UserError {
		Map<String, Double> reference = new HashMap<>();
		for (SyncReplayResult res : repRes) {
			List<Integer> listArray = convertIntListToArray(res.getTraceIndex().toString());
			double alignmentCosts = res.getInfo().get(PNRepResult.RAWFITNESSCOST);
			for (Integer index : listArray) {
				// get the right trace
				String name = XConceptExtension.instance().extractName(getXLog().get(index));
				reference.put(name, alignmentCosts);
			}
		}
		return reference;
	}

	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: replay log on petri net using incremental prefix alignments");
		long startTime = System.currentTimeMillis();

		fullAlignmentCosts.putAll(
				constructFullAlignmentsReference(inputPNRepRes.getData(PNRepResultIOObject.class).getArtifact()));

		PetriNetIOObject netIOObj = inputPN.getData(PetriNetIOObject.class);
		Petrinet net = netIOObj.getArtifact();
		Marking initialMarking, finalMarking = null;
		try {
			initialMarking = netIOObj.getInitialMarking();
			finalMarking = netIOObj.getFinalMarking();
		} catch (ObjectNotFoundException e) {
			throw new OperatorException("No marking(s) found!", e);
		}

		Map<Transition, String> modelElementsToLabelMap = new HashMap<>();
		Map<String, Collection<Transition>> labelsToModelElementsMap = new HashMap<>();
		setupModelBasedLabelMaps(net, modelElementsToLabelMap, labelsToModelElementsMap);
		TObjectDoubleMap<Transition> modelMoveCosts = new TObjectDoubleHashMap<>();
		setupModelMoveCosts(net, modelMoveCosts);
		IncrementalRevBasedReplayerParametersImpl<Petrinet, String, Transition> parameters = new IncrementalRevBasedReplayerParametersImpl<>();
		TObjectDoubleMap<String> labelMoveCosts = new TObjectDoubleHashMap<>();
		parameters.setUseMultiThreading(false);
		parameters.setLabelMoveCosts(labelMoveCosts);
		parameters.setLabelToModelElementsMap(labelsToModelElementsMap);
		parameters.setModelMoveCosts(modelMoveCosts);
		parameters.setModelElementsToLabelMap(modelElementsToLabelMap);
		parameters.setSearchAlgorithm(IncrementalReplayer.SearchAlgorithm.A_STAR);
		parameters.setExperiment(true);
		parameters.setUseSolutionUpperBound(getParameterAsBoolean(PARAMETER_KEY_UPBO));
		int lookBacks = getParameterAsInt(PARAMETER_KEY_MAX_LOOKBACK);
		lookBacks = lookBacks == -1 ? Integer.MAX_VALUE : lookBacks;
		parameters.setLookBackWindow(lookBacks);

		outputAlignments.deliver(replay(net, initialMarking, finalMarking, getXLog(), getXEventClassifier(), parameters,
				fullAlignmentCosts));

		logger.log(Level.INFO, "End: replay log on petri net for conformance checking ("
				+ (System.currentTimeMillis() - startTime) / 1000 + " sec)");
	}

	public DataRowFactory getDataRowFactory() {
		return dataRowFactory;
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		parameterTypes.add(new ParameterTypeBoolean(PARAMETER_KEY_UPBO, PARAMETER_DESC_UPBO, true, true));

		parameterTypes.add(new ParameterTypeInt(PARAMETER_KEY_MAX_LOOKBACK, PARAMETER_DESC_MAX_LOOKBACK, -1,
				Integer.MAX_VALUE, Integer.MAX_VALUE, true));

		return parameterTypes;
	}

	private ExampleSet replay(final Petrinet net, final Marking initialMarking, final Marking finalMarking,
			final XLog log, final XEventClassifier classifier,
			final IncrementalRevBasedReplayerParametersImpl<Petrinet, String, Transition> parameters,
			Map<String, Double> fullAlignmentCosts) throws OperatorException {
		ModelSemanticsPetrinet<Marking> modelSemantics = ModelSemanticsPetrinet.Factory.construct(net);
		Map<Transition, String> labelsInPN = new HashMap<Transition, String>();
		for (Transition t : net.getTransitions()) {
			if (!t.isInvisible()) {
				labelsInPN.put(t, t.getLabel());
			}
		}
		Map<String, MeasurementAwarePartialAlignment<String, Transition, Marking>> store = new HashMap<>();
		IncrementalReplayer<Petrinet, String, Marking, Transition, String, MeasurementAwarePartialAlignment<String, Transition, Marking>, IncrementalRevBasedReplayerParametersImpl<Petrinet, String, Transition>> replayer = IncrementalReplayer.Factory
				.construct(initialMarking, finalMarking, store, modelSemantics, parameters, labelsInPN,
						IncrementalReplayer.Strategy.REVERT_BASED);
		XEventClasses classes = XEventClasses.deriveEventClasses(classifier, log);
		final TObjectDoubleMap<String> costPerTrace = new TObjectDoubleHashMap<>();
		final TObjectIntMap<String> traceCount = new TObjectIntHashMap<>();
		IncrementalReplayResult<String, String, Transition, Marking, MeasurementAwarePartialAlignment<String, Transition, Marking>> replayResult = IncrementalReplayResult.Factory
				.construct(IncrementalReplayResult.Impl.HASH_MAP);
		final Map<String, List<Double>> conventionalCostsPerPrefixLength = new HashMap<>();
		for (XTrace t : log) {
			if (!t.isEmpty()) {
				String caseId = XConceptExtension.instance().extractName(t);
				if (!caseId.contains(PrefixClosureOperatorImpl.PREFIX_NAME_SUFFIX)) {
					List<String> traceStrLst = toStringList(t, classes);
					String traceStr = StringUtils.join(traceStrLst, ",");
					if (!costPerTrace.containsKey(traceStr)) {
						replayResult.put(caseId,
								new ArrayList<MeasurementAwarePartialAlignment<String, Transition, Marking>>());
						conventionalCostsPerPrefixLength.put(caseId, new ArrayList<Double>());
						PartialAlignment<String, Transition, Marking> partialAlignment = null;
						int prefixLength = 1;
						for (String e : traceStrLst) {
							partialAlignment = replayer.processEvent(caseId, e.toString());
							replayResult.get(caseId).add(
									(MeasurementAwarePartialAlignment<String, Transition, Marking>) partialAlignment);
							if (prefixLength < traceStrLst.size()) {
								conventionalCostsPerPrefixLength.get(caseId)
										.add(fetchConventionalCostsForPrefixOfLength(caseId, prefixLength));
								prefixLength++;
							} else if (prefixLength == traceStrLst.size()) {
								conventionalCostsPerPrefixLength.get(caseId).add(fullAlignmentCosts.get(caseId));
							} else {
								throw new OperatorException("PREFIX LENGTH MSIMATCH");
							}

						}
						costPerTrace.put(traceStr, partialAlignment.getCost());
						traceCount.put(traceStr, 1);
					} else {
						traceCount.adjustOrPutValue(traceStr, 1, 1);
					}
				}

			}
		}
		return constructExampleSet(replayResult, traceCount, fullAlignmentCosts, conventionalCostsPerPrefixLength);
	}

	private double fetchConventionalCostsForPrefixOfLength(String caseId, int prefixLength) {
		final String prefixName = caseId + PrefixClosureOperatorImpl.PREFIX_NAME_SUFFIX + prefixLength;
		return fullAlignmentCosts.get(prefixName);
	}

	private void setupModelBasedLabelMaps(final Petrinet net, Map<Transition, String> modelElementsToLabelMap,
			Map<String, Collection<Transition>> labelsToModelElementsMap) {
		for (Transition t : net.getTransitions()) {
			if (!t.isInvisible()) {
				String label = t.getLabel();
				modelElementsToLabelMap.put(t, label);
				if (!labelsToModelElementsMap.containsKey(label)) {
					labelsToModelElementsMap.put(label, Collections.singleton(t));
				} else {
					labelsToModelElementsMap.get(label).add(t);
				}
			}
		}
	}

	private void setupModelMoveCosts(final Petrinet net, final TObjectDoubleMap<Transition> modelMoveCosts) {
		for (Transition t : net.getTransitions()) {
			if (t.isInvisible()) {
				modelMoveCosts.put(t, 0d);
			} else {
				modelMoveCosts.put(t, 1d);
			}
		}
	}

	private List<String> toStringList(XTrace trace, XEventClasses classes) {
		List<String> l = new ArrayList<>(trace.size());
		for (int i = 0; i < trace.size(); i++) {
			l.add(i, classes.getByIdentity(XConceptExtension.instance().extractName(trace.get(i))).toString());
		}
		return l;
	}

}
