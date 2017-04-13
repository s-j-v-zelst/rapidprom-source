package org.rapidprom.operators.filtering;


import java.io.IOException;
import java.util.List;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logfiltering.parameters.SequenceFilterParameter;
import org.processmining.logfiltering.plugins.SequenceFilterPlugin;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.filtering.abstr.AbstractFilteringOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;

public class SequenceFilterOperatorImpl extends AbstractFilteringOperator {

	private static final String Minimum_HighFrequentPatterns = "Minimum Support For High frequency patterns";
	private static final String PARAMETER_DESC_Minimum_HighFrequentPatterns = "Set the Minimum Support For High frequency patterns which is used in find high frequency patterns";

	private static final String Minimum_Confidence_HighFrequentRules = "Minimum Confidence For High frequency Rules";
	private static final String PARAMETER_DESC_Minimum_Confidence_HighFrequentRules= "Set the Minimum Confidence For High frequency Rules which is used in find high frequency patterns";

	private static final String Minimum_Support_HighFrequentRules = "Minimum Support For High frequency Rules";
	private static final String PARAMETER_DESC_Support_Minimum_HighFrequentRules= "Set the Minimum Support For High frequency Rules which is used in find high frequency patterns";

	private static final String Minimum_Support_OrdinaryRules = "Minimum Support For Ordinary Rules";
	private static final String PARAMETER_DESC_Minimum_Support_OrdinaryRules= "Set the Minimum Support For Ordinary Rules which is used in find high frequency patterns";

	private static final String Minimum_Confidance_OrdinaryRules = "Minimum Confidence For Ordinary Rules";
	private static final String PARAMETER_DESC_Minimum_Confidance_OrdinaryRules= "Set the Minimum Confidence For Ordinary Rules which is used in find high frequency patterns";

	public SequenceFilterOperatorImpl(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		XLog noisyLog = getInputLogPort().getData(XLogIOObject.class).getArtifact();

		SequenceFilterParameter parameters = new SequenceFilterParameter(
				getXEventClassifier());
		parameters.setHighSupportPattern(getParameterAsDouble(Minimum_HighFrequentPatterns));
		parameters.setConfHighConfRules(getParameterAsDouble(Minimum_Confidence_HighFrequentRules));
		parameters.setSuppHighConfRules(getParameterAsDouble(Minimum_Support_HighFrequentRules));
		parameters.setSuppOrdinaryRules(getParameterAsDouble(Minimum_Support_OrdinaryRules));		
		parameters.setConfOridnaryRules(getParameterAsDouble(Minimum_Confidance_OrdinaryRules));
			
				

		PluginContext context = RapidProMGlobalContext.instance().getPluginContext();
		try {
			getOutputLogPort().deliver(new XLogIOObject(SequenceFilterPlugin.run(context, noisyLog, parameters), context));
		} catch (IOException e) {
		}
		
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeDouble(Minimum_HighFrequentPatterns, PARAMETER_DESC_Minimum_HighFrequentPatterns, 0, 1, 0.9,
				false));
		params.add(new ParameterTypeDouble(Minimum_Confidence_HighFrequentRules, PARAMETER_DESC_Minimum_Confidence_HighFrequentRules, 0, 1, 0.8,
				false));
		params.add(new ParameterTypeDouble(Minimum_Support_HighFrequentRules, PARAMETER_DESC_Support_Minimum_HighFrequentRules, 0, 1, 0.2,
				false));
		params.add(new ParameterTypeDouble(Minimum_Support_OrdinaryRules, PARAMETER_DESC_Minimum_Support_OrdinaryRules, 0, 1, 0.15,
				false));
		params.add(new ParameterTypeDouble(Minimum_Confidance_OrdinaryRules, PARAMETER_DESC_Minimum_Confidance_OrdinaryRules, 0, 1, 0.2,
				false));
		return params;
	}

}
