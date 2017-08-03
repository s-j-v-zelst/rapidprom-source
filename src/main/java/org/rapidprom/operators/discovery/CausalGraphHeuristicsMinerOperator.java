package org.rapidprom.operators.discovery;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.model.XLog;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicsCausalGraphBuilder.HeuristicsConfig;
import org.processmining.dataawarecnetminer.mining.classic.HeuristicsCausalGraphMiner;
import org.processmining.models.causalgraph.SimpleCausalGraph;
import org.processmining.models.causalgraph.XEventClassifierAwareSimpleCausalGraph;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XEventClassifierAwareSimpleCausalGraphIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.tools.LogService;

public class CausalGraphHeuristicsMinerOperator extends AbstractRapidProMEventLogBasedOperator {

	private static final String PARAM_KEY_FREQ = "frequency";
	private static final String PARAM_DESC_FREQ = "Frequency Threshold";
	private static final double PARAM_DEFAULT_FREQ = 0.2d;

	private static final String PARAM_KEY_DEP = "dependency";
	private static final String PARAM_DESC_DEP = "Dependency Threshold";
	private static final double PARAM_DEFAULT_DEP = 0.9d;

	private static final String PARAM_KEY_L1 = "l1";
	private static final String PARAM_DESC_L1 = "L1-Loop Threshold";
	private static final double PARAM_DEFAULT_L1 = 0.9d;

	private static final String PARAM_KEY_L2 = "l2";
	private static final String PARAM_DESC_L2 = "L2-Loop Threshold";
	private static final double PARAM_DEFAULT_L2 = 0.9d;

	private static final String PARAM_KEY_ALL_CONN = "all_conn";
	private static final String PARAM_DESC_ALL_CONN = "All Tasks Connected";
	private static final boolean PARAM_DEFAULT_ALL_CONN = false;

	private static final String PARAM_KEY_ACC_CONN = "acc_conn";
	private static final String PARAM_DESC_ACC_CONN = "Accepted Tasks Connected";
	private static final boolean PARAM_DEFAULT_ACC_CONN = true;

	private OutputPort output = getOutputPorts().createPort("model (Causal Graph)");

	public CausalGraphHeuristicsMinerOperator(OperatorDescription description) {
		super(description);

		/** Adding a rule for the output */
		getTransformer().addRule(new GenerateNewMDRule(output, XEventClassifierAwareSimpleCausalGraphIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "start: discover causal graph");
		long time = System.currentTimeMillis();

		HeuristicsConfig heuristicsConfig = new HeuristicsConfig();
		heuristicsConfig.setObservationThreshold(getParameterAsDouble(PARAM_KEY_FREQ));
		heuristicsConfig.setDependencyThreshold(getParameterAsDouble(PARAM_KEY_DEP));
		heuristicsConfig.setL1Threshold(getParameterAsDouble(PARAM_KEY_L1));
		heuristicsConfig.setL2Threshold(getParameterAsDouble(PARAM_KEY_L2));
		heuristicsConfig.setAllTasksConnected(getParameterAsBoolean(PARAM_KEY_ALL_CONN));
		heuristicsConfig.setAcceptedTasksConnected(getParameterAsBoolean(PARAM_KEY_ACC_CONN));

		SimpleCausalGraph scag = doMineCausalGraph(getXLog(), getXEventClassifier(), heuristicsConfig);

		XEventClassifierAwareSimpleCausalGraph xescag = XEventClassifierAwareSimpleCausalGraph.Factory
				.construct(getXEventClassifier(), scag.getSetActivities(), scag.getCausalRelations());

		XEventClassifierAwareSimpleCausalGraphIOObject iooutput = new XEventClassifierAwareSimpleCausalGraphIOObject(
				xescag, RapidProMGlobalContext.instance().getPluginContext());

		output.deliver(iooutput);

		logger.log(Level.INFO, "end: discover causal graph (" + (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

	private static SimpleCausalGraph doMineCausalGraph(XLog log, XEventClassifier classifier,
			HeuristicsConfig heuristicsConfig) {
		HeuristicsCausalGraphMiner miner = new HeuristicsCausalGraphMiner(log, classifier);
		miner.setHeuristicsConfig(heuristicsConfig);
		return miner.mineCausalGraph();
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		final List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeDouble freq = new ParameterTypeDouble(PARAM_KEY_FREQ, PARAM_DESC_FREQ, 0, 1, PARAM_DEFAULT_FREQ,
				false);
		parameterTypes.add(freq);

		ParameterTypeDouble dep = new ParameterTypeDouble(PARAM_KEY_DEP, PARAM_DESC_DEP, 0, 1, PARAM_DEFAULT_DEP,
				false);
		parameterTypes.add(dep);

		ParameterTypeDouble l1 = new ParameterTypeDouble(PARAM_KEY_L1, PARAM_DESC_L1, 0, 1, PARAM_DEFAULT_L1, false);
		parameterTypes.add(l1);

		ParameterTypeDouble l2 = new ParameterTypeDouble(PARAM_KEY_L2, PARAM_DESC_L2, 0, 1, PARAM_DEFAULT_L2, false);
		parameterTypes.add(l2);

		ParameterTypeBoolean allConn = new ParameterTypeBoolean(PARAM_KEY_ALL_CONN, PARAM_DESC_ALL_CONN,
				PARAM_DEFAULT_ALL_CONN, false);
		parameterTypes.add(allConn);

		ParameterTypeBoolean accConn = new ParameterTypeBoolean(PARAM_KEY_ACC_CONN, PARAM_DESC_ACC_CONN,
				PARAM_DEFAULT_ACC_CONN, false);
		parameterTypes.add(accConn);

		return parameterTypes;
	}

}
