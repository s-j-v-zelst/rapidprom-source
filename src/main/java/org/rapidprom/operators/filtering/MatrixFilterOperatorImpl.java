package org.rapidprom.operators.filtering;

import java.util.List;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logfiltering.parameters.MatrixFilterParameter;
import org.processmining.logfiltering.plugins.MatrixFilterPlugin;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.filtering.abstr.AbstractFilteringOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;

public class MatrixFilterOperatorImpl extends AbstractFilteringOperator {

	private static final String PARAMETER_KEY_FILTER_THRESHOLD = "Threshold";
	private static final String PARAMETER_DESC_FILTER_THRESHOLD = "Set the filtering threshold which is used for outlier detection";
	private static final String PARAMETER_KEY_SUBSEQUENT_LENGTH = "Subsequent Length";
	private static final String PARAMETER_DESC_SUBSEQUENT_LENGTH = "Set the subsequent length which is used for outlier detection";

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
		PluginContext context = RapidProMGlobalContext.instance().getPluginContext();
		getOutputLogPort().deliver(new XLogIOObject(MatrixFilterPlugin.run(context, noisyLog, parameters), context));
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeDouble(PARAMETER_KEY_FILTER_THRESHOLD, PARAMETER_DESC_FILTER_THRESHOLD, 0, 1, 0.1,
				false));
		params.add(new ParameterTypeInt(PARAMETER_KEY_SUBSEQUENT_LENGTH, PARAMETER_DESC_SUBSEQUENT_LENGTH, 0, 100, 2,
				false));
		return params;
	}

}
