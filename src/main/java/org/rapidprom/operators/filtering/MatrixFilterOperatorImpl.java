package org.rapidprom.operators.filtering;

import java.util.List;

import javax.persistence.GenerationType;

import org.deckfour.xes.model.XLog;
import org.jbpt.petri.unfolding.order.AdequateOrderType;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.hybridilpminer.parameters.LPFilterType;
import org.processmining.logfiltering.parameters.AdjustingType;
import org.processmining.logfiltering.parameters.MatrixFilterParameter;
import org.processmining.logfiltering.plugins.MatrixFilterPlugin;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.filtering.abstr.AbstractFilteringOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;

public class MatrixFilterOperatorImpl extends AbstractFilteringOperator {

	private static final String PARAMETER_KEY_FILTER_THRESHOLD = "Threshold";
	private static final String PARAMETER_DESC_FILTER_THRESHOLD = "Set the filtering threshold which is used for outlier detection";
	private static final String PARAMETER_KEY_SUBSEQUENT_LENGTH = "Subsequent Length";
	private static final String PARAMETER_DESC_SUBSEQUENT_LENGTH = "Set the subsequent length which is used for outlier detection";
	private static final String PARAMETER_KEY_Adjustment_Method = "Threshold Adjustment";
	private static final String PARAMETER_DESC_Adjustment_Method = "Set the Threshold Adjustment Method";
	private static final String PARAMETER_DESC_Probability_Method = "Set the method of detecting behavioural probabilities (values)";
	private static AdjustingType none= AdjustingType.None;private static AdjustingType mean= AdjustingType.Mean;
	private static AdjustingType maxmean= AdjustingType.MaxMean;private static AdjustingType maxVmean= AdjustingType.MaxVMean;
	
	private static final String[] Adjustment_OPTIONS = new String[] { none.toString(), mean.toString(),maxmean.toString(),maxVmean.toString() };
	private static final AdjustingType[] Adjustment_OPTIONS_List= new AdjustingType[]{ AdjustingType.None,AdjustingType.Mean, AdjustingType.MaxMean,AdjustingType.MaxVMean};
	public MatrixFilterOperatorImpl(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		XLog noisyLog = getInputLogPort().getData(XLogIOObject.class).getArtifact();

		MatrixFilterParameter parameters = new MatrixFilterParameter(
				getXEventClassifier());
		parameters.setProbabilityOfRemoval(getParameterAsDouble(PARAMETER_KEY_FILTER_THRESHOLD));
		parameters.setSubsequenceLength(getParameterAsInt(PARAMETER_KEY_SUBSEQUENT_LENGTH));
		parameters.setAdjustingThresholdMethod(Adjustment_OPTIONS_List[getParameterAsInt(PARAMETER_KEY_Adjustment_Method)]);
		parameters.setSubsequenceLength(getParameterAsInt(PARAMETER_KEY_SUBSEQUENT_LENGTH));
		PluginContext context = RapidProMGlobalContext.instance().getPluginContext();
		getOutputLogPort().deliver(new XLogIOObject(MatrixFilterPlugin.run(context, noisyLog, parameters), context));
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeDouble(PARAMETER_KEY_FILTER_THRESHOLD, PARAMETER_DESC_FILTER_THRESHOLD, 0, 1, 0.1,
				false));
		params.add(new ParameterTypeInt(PARAMETER_KEY_SUBSEQUENT_LENGTH, PARAMETER_DESC_SUBSEQUENT_LENGTH, 1, 10, 2,
				false));
		params.add(new ParameterTypeCategory(PARAMETER_KEY_Adjustment_Method, PARAMETER_DESC_Adjustment_Method, Adjustment_OPTIONS, 0,
				false));
		return params;
	}

}
