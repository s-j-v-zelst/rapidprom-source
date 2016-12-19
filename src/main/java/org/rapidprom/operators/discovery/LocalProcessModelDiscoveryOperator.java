package org.rapidprom.operators.discovery;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.lpm.dialogs.LocalProcessModelParameters;
import org.processmining.lpm.discovery.LocalProcessModelDiscovery;
import org.processmining.lpm.util.LocalProcessModelRanking;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.LocalProcessModelRankingIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.LogService;

public class LocalProcessModelDiscoveryOperator extends AbstractRapidProMDiscoveryOperator{
	OutputPort output = getOutputPorts().createPort("model (ProM ProcessTree)");
	LocalProcessModelParameters lpmp;
	
	// Parameter keys (also used as description)
	public static final String 
			PARAMETER_1_KEY = "Maximum number of transitions in LPMs",
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

			PARAMETER_11_KEY = "Log projections method",
			PARAMETER_11_DESCR = "Projection method used on the event log to speed up discovery of LPMs",
			
			PARAMETER_12_KEY = "Weight of support metric",
			PARAMETER_12_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_13_KEY = "Weight of language fit metric",
			PARAMETER_13_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_14_KEY = "Weight of confidence metric",
			PARAMETER_14_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_15_KEY = "Weight of coverage metric",
			PARAMETER_15_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_16_KEY = "Weight of determinism metric",
			PARAMETER_16_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs",
			PARAMETER_17_KEY = "Weight of average number of firings metric",
			PARAMETER_17_DESCR = "Relative weights of the metrics are used to rank the discovered LPMs";
	
	public LocalProcessModelDiscoveryOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, LocalProcessModelRankingIOObject.class));
	}
	
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: lpm discovery");
		long time = System.currentTimeMillis();

		lpmp = getConfiguration(getXLog());

		LocalProcessModelDiscovery lpmd = new LocalProcessModelDiscovery();
		PluginContext pluginContext = RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(LocalProcessModelDiscovery.class);		
		
		LocalProcessModelRanking result = lpmd.runHeadless(pluginContext, lpmp);
		
		LocalProcessModelRankingIOObject resultObject = new LocalProcessModelRankingIOObject(result, pluginContext);

		output.deliver(resultObject);

		logger.log(Level.INFO, "End: lpm discovery (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		if(lpmp==null)
			lpmp = new LocalProcessModelParameters();
		ParameterTypeInt parameter1 = new ParameterTypeInt(PARAMETER_1_KEY, PARAMETER_1_DESCR, 1, 5, lpmp.getNumTransitions());
		parameter1.setExpert(false);
		parameterTypes.add(parameter1);
		ParameterTypeInt parameter2 = new ParameterTypeInt(PARAMETER_2_KEY, PARAMETER_2_DESCR, 0, 500, lpmp.getTop_k());
		parameter2.setExpert(false);
		parameterTypes.add(parameter2);
		ParameterTypeBoolean parameter3 = new ParameterTypeBoolean(PARAMETER_3_KEY, PARAMETER_3_DESCR, lpmp.isDuplicateTransitions());
		parameter3.setExpert(true);
		parameterTypes.add(parameter3);
		
		ParameterTypeBoolean parameter4 = new ParameterTypeBoolean(PARAMETER_4_KEY, PARAMETER_4_DESCR, lpmp.isUseSeq());
		parameter4.setExpert(false);
		parameterTypes.add(parameter4);
		ParameterTypeBoolean parameter5 = new ParameterTypeBoolean(PARAMETER_5_KEY, PARAMETER_5_DESCR, lpmp.isUseAnd());
		parameter5.setExpert(false);
		parameterTypes.add(parameter5);
		ParameterTypeBoolean parameter6 = new ParameterTypeBoolean(PARAMETER_6_KEY, PARAMETER_6_DESCR, lpmp.isUseOr());
		parameter6.setExpert(true);
		parameterTypes.add(parameter6);
		ParameterTypeBoolean parameter7 = new ParameterTypeBoolean(PARAMETER_7_KEY, PARAMETER_7_DESCR, lpmp.isUseXor());
		parameter7.setExpert(false);
		parameterTypes.add(parameter7);
		ParameterTypeBoolean parameter8 = new ParameterTypeBoolean(PARAMETER_8_KEY, PARAMETER_8_DESCR, lpmp.isUseXorloop());
		parameter8.setExpert(false);
		parameterTypes.add(parameter8);

		ParameterTypeInt parameter9 = new ParameterTypeInt(PARAMETER_9_KEY, PARAMETER_9_DESCR, 0, Integer.MAX_VALUE, lpmp.getFrequencyMinimum());
		parameter9.setExpert(false);
		parameterTypes.add(parameter9);
		ParameterTypeDouble parameter10 = new ParameterTypeDouble(PARAMETER_10_KEY, PARAMETER_10_DESCR, 0, 1, lpmp.getDeterminismMinimum());
		parameter10.setExpert(true);
		parameterTypes.add(parameter10);

		LocalProcessModelParameters.ProjectionMethods[] methods = LocalProcessModelParameters.ProjectionMethods.values();
		String[] descriptions = new String[methods.length];
		for(int i=0; i<methods.length; i++)
			descriptions[i] = methods[i].toString();
		ParameterTypeCategory parameter11 = new ParameterTypeCategory(PARAMETER_11_KEY, PARAMETER_11_DESCR, descriptions, 1);
		parameter11.setExpert(false);
		parameterTypes.add(parameter11);
		
		ParameterTypeDouble parameter12 = new ParameterTypeDouble(PARAMETER_12_KEY, PARAMETER_12_DESCR, 0, 1, lpmp.getSupportWeight());
		parameter12.setExpert(false);
		parameterTypes.add(parameter12);
		ParameterTypeDouble parameter13 = new ParameterTypeDouble(PARAMETER_13_KEY, PARAMETER_13_DESCR, 0, 1, lpmp.getLanguageFitWeight());
		parameter13.setExpert(false);
		parameterTypes.add(parameter13);
		ParameterTypeDouble parameter14 = new ParameterTypeDouble(PARAMETER_14_KEY, PARAMETER_14_DESCR, 0, 1, lpmp.getConfidenceWeight());
		parameter14.setExpert(false);
		parameterTypes.add(parameter14);
		ParameterTypeDouble parameter15 = new ParameterTypeDouble(PARAMETER_15_KEY, PARAMETER_15_DESCR, 0, 1, lpmp.getCoverageWeight());
		parameter15.setExpert(false);
		parameterTypes.add(parameter15);
		ParameterTypeDouble parameter16 = new ParameterTypeDouble(PARAMETER_16_KEY, PARAMETER_16_DESCR, 0, 1, lpmp.getDeterminismWeight());
		parameter16.setExpert(false);
		parameterTypes.add(parameter16);
		ParameterTypeDouble parameter17 = new ParameterTypeDouble(PARAMETER_17_KEY, PARAMETER_17_DESCR, 0, 1, lpmp.getAvgNumFiringsWeight());
		parameter17.setExpert(true);
		parameterTypes.add(parameter17);		
		
		return parameterTypes;
	}

	private LocalProcessModelParameters getConfiguration(XLog log) {
		try {
			lpmp.setDiscoveryLog(log);
			lpmp.setEvaluationLog(log);
			
			lpmp.setNumTransitions(getParameterAsInt(PARAMETER_1_KEY));
			lpmp.setTop_k(getParameterAsInt(PARAMETER_2_KEY));
			lpmp.setDuplicateTransitions(getParameterAsBoolean(PARAMETER_3_KEY));
			
			lpmp.setUseSeq(getParameterAsBoolean(PARAMETER_4_KEY));
			lpmp.setUseAnd(getParameterAsBoolean(PARAMETER_5_KEY));
			lpmp.setUseOr(getParameterAsBoolean(PARAMETER_6_KEY));
			lpmp.setUseXor(getParameterAsBoolean(PARAMETER_7_KEY));
			lpmp.setUseXorloop(getParameterAsBoolean(PARAMETER_8_KEY));
			
			lpmp.setFrequencyMinimum(getParameterAsInt(PARAMETER_9_KEY));
			lpmp.setDeterminismMinimum(getParameterAsDouble(PARAMETER_10_KEY));
			
			if (getParameterAsString(PARAMETER_11_KEY).matches(LocalProcessModelParameters.ProjectionMethods.Markov.toString()))
				lpmp.setProjectionMethod(LocalProcessModelParameters.ProjectionMethods.Markov);
			else if (getParameterAsString(PARAMETER_11_KEY).matches(LocalProcessModelParameters.ProjectionMethods.MRIG.toString()))
				lpmp.setProjectionMethod(LocalProcessModelParameters.ProjectionMethods.MRIG);
			else if (getParameterAsString(PARAMETER_11_KEY).matches(LocalProcessModelParameters.ProjectionMethods.Entropy.toString()))
				lpmp.setProjectionMethod(LocalProcessModelParameters.ProjectionMethods.Entropy);
			else if (getParameterAsString(PARAMETER_11_KEY).matches(LocalProcessModelParameters.ProjectionMethods.None.toString()))
				lpmp.setProjectionMethod(LocalProcessModelParameters.ProjectionMethods.None);
			
			System.out.println("projection method: "+lpmp.getProjectionMethod());
			
			lpmp.setSupportWeight(getParameterAsDouble(PARAMETER_12_KEY));
			lpmp.setLanguageFitWeight(getParameterAsDouble(PARAMETER_13_KEY));
			lpmp.setConfidenceWeight(getParameterAsDouble(PARAMETER_14_KEY));
			lpmp.setCoverageWeight(getParameterAsDouble(PARAMETER_15_KEY));
			lpmp.setDeterminismWeight(getParameterAsDouble(PARAMETER_16_KEY));
			lpmp.setAvgNumFiringsWeight(getParameterAsDouble(PARAMETER_17_KEY));
		} catch (UndefinedParameterError e) {
			e.printStackTrace();
		}
		return lpmp;
	}
}
