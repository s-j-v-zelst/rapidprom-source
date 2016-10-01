package org.rapidprom.operators.discovery;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.model.XLog;
import org.processmining.acceptingpetrinet.models.AcceptingPetriNetArray;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.lpm.dialogs.LocalProcessModelParameters;
import org.processmining.lpm.discovery.LocalProcessModelDiscovery;
import org.processmining.lpm.plugins.UnpackAlignmentScoredAcceptingPetriNetArrayImpl;
import org.processmining.lpm.util.AlignmentScoredAcceptingPetriNetArrayImpl;
import org.processmining.plugins.heuristicsnet.miner.heuristics.miner.FlexibleHeuristicsMinerPlugin;
import org.processmining.plugins.heuristicsnet.miner.heuristics.miner.settings.HeuristicsMinerSettings;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AcceptingPetriNetArrayIOObject;
import org.rapidprom.ioobjects.HeuristicsNetIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.ProcessTreeIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.LogService;

public class LocalProcessModelDiscoveryOperator extends AbstractRapidProMDiscoveryOperator{
	OutputPort output = getOutputPorts().createPort("model (ProM ProcessTree)");
	
	// Parameter keys (also used as description)
	public static final String PARAMETER_1_KEY = "Maximum number of transitions in LPMs",
			PARAMETER_1_DESCR = "Maximum number of non-silent transitions in LPMs. Higher values lead to higher computation time. A value of at most 4 is advised.",
			PARAMETER_2_KEY = "Maximum number of LPMs to discover",
			PARAMETER_2_DESCR = "Maximum size of result list of LPMs. Has no influence on computation time.",
			PARAMETER_3_KEY = "Allow duplicate transitions",
			PARAMETER_3_DESCR = "Whether to allow LPMs to contain duplicate transitions. This significantly increases computation time.",
			PARAMETER_4_KEY = "Use sequence operator",
			PARAMETER_4_DESCR = "Whether to allow resulting LPMs to contain sequence constructs",
			PARAMETER_5_KEY = "Use concurrency operator",
			PARAMETER_5_DESCR = "Whether to allow resulting LPMs to contain concurrency constructs",
			PARAMETER_6_KEY = "Use inclusive choice operator",
			PARAMETER_6_DESCR = "Whether to allow resulting LPMs to contain inclusive choice constructs",
			PARAMETER_7_KEY = "Use exclusive choice operator",
			PARAMETER_7_DESCR = "Whether to allow resulting LPMs to contain exclusive choice constructs",
			PARAMETER_8_KEY = "Use loop operator",
			PARAMETER_8_DESCR = "Whether to allow resulting LPMs to contain loop constructs",
			PARAMETER_9_KEY = "Minimum occurrences of LPMs",
			PARAMETER_9_DESCR = "Minimum number of occurrences of an LPM in the log. The set value is used for pruning, therefore higher values lead to lower computation time.",
			PARAMETER_10_KEY = "Minimum determinicsm",
			PARAMETER_10_DESCR = "Minimum value of the determinism metric for LPMs. The set value is used for pruning, therefore higher values lead to lower computation time.",
			PARAMETER_11_KEY = "Weight of support metric",
			PARAMETER_11_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_12_KEY = "Weight of language fit metric",
			PARAMETER_12_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_13_KEY = "Weight of confidence metric",
			PARAMETER_13_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_14_KEY = "Weight of coverage metric",
			PARAMETER_14_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_15_KEY = "Weight of determinism metric",
			PARAMETER_15_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_16_KEY = "Weight of average number of firings metric",
			PARAMETER_16_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs";
	
	public LocalProcessModelDiscoveryOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, ProcessTreeIOObject.class));
	}
	
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: lpm discovery");
		long time = System.currentTimeMillis();

		LocalProcessModelParameters lpmDiscoverySettings = getConfiguration(getXLog());

		LocalProcessModelDiscovery lpmd = new LocalProcessModelDiscovery();
		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		AlignmentScoredAcceptingPetriNetArrayImpl result = lpmd.runHeadless(pluginContext, lpmDiscoverySettings);
		UnpackAlignmentScoredAcceptingPetriNetArrayImpl unpacker = new UnpackAlignmentScoredAcceptingPetriNetArrayImpl();
		AcceptingPetriNetArray array = unpacker.unpack(pluginContext, result);
		
		AcceptingPetriNetArrayIOObject resultObject = new AcceptingPetriNetArrayIOObject(array, pluginContext);

		output.deliver(resultObject);

		logger.log(Level.INFO, "End: lpm discovery (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeDouble parameter1 = new ParameterTypeDouble(PARAMETER_1_KEY, PARAMETER_1_DESCR, 0, 100, 5);
		parameterTypes.add(parameter1);

		ParameterTypeDouble parameter2 = new ParameterTypeDouble(PARAMETER_2_KEY, PARAMETER_2_DESCR, 0, 100, 90);
		parameterTypes.add(parameter2);

		ParameterTypeDouble parameter3 = new ParameterTypeDouble(PARAMETER_3_KEY, PARAMETER_3_DESCR, 0, 100, 90);
		parameterTypes.add(parameter3);

		ParameterTypeDouble parameter4 = new ParameterTypeDouble(PARAMETER_4_KEY, PARAMETER_4_DESCR, 0, 100, 90);
		parameterTypes.add(parameter4);

		ParameterTypeDouble parameter5 = new ParameterTypeDouble(PARAMETER_5_KEY, PARAMETER_5_DESCR, 0, 100, 90);
		parameterTypes.add(parameter5);

		ParameterTypeBoolean parameter6 = new ParameterTypeBoolean(PARAMETER_6_KEY, PARAMETER_6_DESCR, true);
		parameterTypes.add(parameter6);

		ParameterTypeBoolean parameter7 = new ParameterTypeBoolean(PARAMETER_7_KEY, PARAMETER_6_DESCR, false);
		parameterTypes.add(parameter7);

		return parameterTypes;
	}

	private LocalProcessModelParameters getConfiguration(XLog log) {
		LocalProcessModelParameters lpmDiscoverySettings = new LocalProcessModelParameters();
		try {
			lpmDiscoverySettings.setNumTransitions(getParameterAsInt(PARAMETER_1_KEY));
			lpmDiscoverySettings.setTop_k(getParameterAsInt(PARAMETER_2_KEY));
			lpmDiscoverySettings.setDuplicateTransitions(getParameterAsBoolean(PARAMETER_3_KEY));
			
			lpmDiscoverySettings.setUseSeq(getParameterAsBoolean(PARAMETER_4_KEY));
			lpmDiscoverySettings.setUseAnd(getParameterAsBoolean(PARAMETER_5_KEY));
			lpmDiscoverySettings.setUseOr(getParameterAsBoolean(PARAMETER_6_KEY));
			lpmDiscoverySettings.setUseXor(getParameterAsBoolean(PARAMETER_7_KEY));
			lpmDiscoverySettings.setUseXorloop(getParameterAsBoolean(PARAMETER_8_KEY));
			
			lpmDiscoverySettings.setFrequencyMinimum(getParameterAsInt(PARAMETER_9_KEY));
			lpmDiscoverySettings.setDeterminismMinimum(getParameterAsDouble(PARAMETER_10_KEY));
			
			lpmDiscoverySettings.setSupportWeight(getParameterAsDouble(PARAMETER_11_KEY));
			lpmDiscoverySettings.setLanguageFitWeight(getParameterAsDouble(PARAMETER_12_KEY));
			lpmDiscoverySettings.setConfidenceWeight(getParameterAsDouble(PARAMETER_13_KEY));
			lpmDiscoverySettings.setCoverageWeight(getParameterAsDouble(PARAMETER_14_KEY));
			lpmDiscoverySettings.setDeterminismWeight(getParameterAsDouble(PARAMETER_15_KEY));
			lpmDiscoverySettings.setAvgNumFiringsWeight(getParameterAsDouble(PARAMETER_16_KEY));
		} catch (UndefinedParameterError e) {
			e.printStackTrace();
		}
		return lpmDiscoverySettings;
	}
}
