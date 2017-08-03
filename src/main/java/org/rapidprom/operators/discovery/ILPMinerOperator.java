package org.rapidprom.operators.discovery;

import java.util.List;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.model.XLog;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicsCausalGraphBuilder.HeuristicsConfig;
import org.processmining.dataawarecnetminer.plugins.CausalGraphMinerPlugin;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.hybridilpminer.parameters.LPFilter;
import org.processmining.hybridilpminer.parameters.LPFilterType;
import org.processmining.hybridilpminer.parameters.XLogHybridILPMinerParametersImpl;
import org.processmining.hybridilpminer.plugins.HybridILPMinerPlugin;
import org.processmining.models.causalgraph.SimpleCausalGraph;
import org.processmining.models.causalgraph.XEventClassifierAwareSimpleCausalGraph;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.semantics.petrinet.Marking;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XEventClassifierAwareSimpleCausalGraphIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.parameter.conditions.EqualStringCondition;

public class ILPMinerOperator extends AbstractRapidProMDiscoveryOperator {

	private OutputPort outputPetrinet = getOutputPorts().createPort("model (ProM Petri Net)");

	private InputPort inputCausalGraph = getInputPorts().createPort("causal graph");

	// private static final String PARAMETER_KEY_EAC =
	// "enforce_emptiness_after_completion";
	// private static final String PARAMETER_DESC_EAC = "Indicates whether the
	// net is empty after replaying the event log";

	// private static final String PARAMETER_KEY_SINK = "find_sink";
	// private static final String PARAMETER_DESC_SINK = "Indicates whether the
	// miner needs to search for a sink place, only works if emptiness after
	// completion is selected";

	private static final String PARAMETER_KEY_FILTER = "Filter";
	private static final String PARAMETER_DESC_FILTER = "We can either apply no filtering, which guarantees perfect replay-fitness, or filter using Sequence Encoding Filtering (SEF)";
	private static final String[] PARAMETER_OPTIONS_FITLER = new String[] { LPFilterType.NONE.toString(),
			LPFilterType.SEQUENCE_ENCODING.toString() };
	private static final LPFilterType[] PARAMETER_REFERENCE_FILTER = new LPFilterType[] { LPFilterType.NONE,
			LPFilterType.SEQUENCE_ENCODING };

	private static final String PARAMETER_KEY_FILTER_THRESHOLD = "Filter Threshold";
	private static final String PARAMETER_DESC_FILTER_THRESHOLD = "Set the sequence encoding threshold t, for which 0 <= t <= 1.";

	public ILPMinerOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputPetrinet, PetriNetIOObject.class));
	}

	public void doWork() throws OperatorException {
		PluginContext context = RapidProMGlobalContext.instance().getPluginContext();
		XLog log = getXLog();
		XEventClassifier classifier = getXEventClassifier();
		XLogHybridILPMinerParametersImpl params = new XLogHybridILPMinerParametersImpl(context, log, classifier);

		params.setFindSink(true);
		params.setFilter(getFilter());

		Object[] pnAndMarking = HybridILPMinerPlugin.applyFlexHeur(context, log, getCausalGraph(), params);

		PetriNetIOObject petrinetIOObject = new PetriNetIOObject((Petrinet) pnAndMarking[0], (Marking) pnAndMarking[1],
				(Marking) pnAndMarking[2], context);
		outputPetrinet.deliver(petrinetIOObject);
	}

	// private Set<LPConstraintType> getConstraintTypes() {
	// Set<LPConstraintType> constraints =
	// EnumSet.of(LPConstraintType.THEORY_OF_REGIONS,
	// LPConstraintType.NO_TRIVIAL_REGION);
	// // if (getParameterAsBoolean(PARAMETER_KEY_EAC)) {
	// constraints.add(LPConstraintType.EMPTY_AFTER_COMPLETION);
	// // }
	// return constraints;
	// }

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		// addEmptinessAfterCompletionParameter(params);

		addFilterParameter(params);
		return params;
	}

	// private List<ParameterType>
	// addEmptinessAfterCompletionParameter(List<ParameterType> params) {
	// ParameterTypeBoolean empty = new ParameterTypeBoolean(PARAMETER_KEY_EAC,
	// PARAMETER_DESC_EAC, true, false);
	// params.add(empty);
	//
	// ParameterTypeBoolean sink = new ParameterTypeBoolean(PARAMETER_KEY_SINK,
	// PARAMETER_DESC_SINK, true, false);
	// sink.setOptional(true);
	// sink.registerDependencyCondition(new BooleanParameterCondition(this,
	// PARAMETER_KEY_EAC, true, true));
	// params.add(sink);
	//
	// return params;
	// }

	private List<ParameterType> addFilterParameter(List<ParameterType> params) {
		params.add(new ParameterTypeCategory(PARAMETER_KEY_FILTER, PARAMETER_DESC_FILTER, PARAMETER_OPTIONS_FITLER, 0,
				false));

		ParameterType filterThreshold = new ParameterTypeDouble(PARAMETER_KEY_FILTER_THRESHOLD,
				PARAMETER_DESC_FILTER_THRESHOLD, 0, 1, 0.25, false);
		filterThreshold.setOptional(true);
		filterThreshold.registerDependencyCondition(
				new EqualStringCondition(this, PARAMETER_KEY_FILTER, true, LPFilterType.SEQUENCE_ENCODING.toString()));

		params.add(filterThreshold);
		return params;
	}

	private LPFilter getFilter() throws UndefinedParameterError {
		LPFilter filter = new LPFilter();
		LPFilterType type = PARAMETER_REFERENCE_FILTER[getParameterAsInt(PARAMETER_KEY_FILTER)];
		filter.setFilterType(type);
		switch (type) {
		case SEQUENCE_ENCODING:
			filter.setThreshold(getParameterAsDouble(PARAMETER_KEY_FILTER_THRESHOLD));
			break;
		default:
			break;
		}
		return filter;
	}

	protected XEventClassifierAwareSimpleCausalGraph getCausalGraph() throws UserError {
		XEventClassifierAwareSimpleCausalGraphIOObject cagIoobj = inputCausalGraph
				.getDataOrNull(XEventClassifierAwareSimpleCausalGraphIOObject.class);
		XEventClassifierAwareSimpleCausalGraph cag = null;
		if (cagIoobj == null) {
			HeuristicsConfig heuristicsConfig = new HeuristicsConfig();
			heuristicsConfig.setAllTasksConnected(true);
			SimpleCausalGraph scag = CausalGraphMinerPlugin.doMineCausalGraph(getXLog(), getXEventClassifier(),
					heuristicsConfig);
			cag = XEventClassifierAwareSimpleCausalGraph.Factory.construct(getXEventClassifier(),
					scag.getSetActivities(), scag.getCausalRelations());
		} else {
			cag = cagIoobj.getArtifact();
		}
		return cag;
	}
}
