package org.rapidprom.operators.logmanipulation;

import java.util.List;
import org.deckfour.xes.model.XLog;
import org.processmining.log.parameters.HighFrequencyFilterParameters;
import org.processmining.log.plugins.HighFrequencyFilterPlugin;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;

/**
 * Filters low frequency traces from the event log.
 *
 */
public class FilterHighFrequencyTracesOperator extends AbstractRapidProMLogManipulationOperator {

	private static final String PARAMETER_FREQUENCY_THRESHOLD_KEY = "Frequency threshold",
			PARAMETER_FREQUENCY_THRESHOLD_DESCR = "Threshold for high-frequency traces.",
			PARAMETER_DISTANCE_THRESHOLD_KEY = "Distance threshold",
			PARAMETER_DISTANCE_THRESHOLD_DESCR = "Distance to low-frequency traces.";

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public FilterHighFrequencyTracesOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		XLog logOriginal = getXLog();
		HighFrequencyFilterPlugin highFrequencyFilterPlugin = new HighFrequencyFilterPlugin();

		// TODO missing constructor without log
		HighFrequencyFilterParameters parameters = new HighFrequencyFilterParameters(logOriginal);
		parameters.setClassifier(getXEventClassifier());

		parameters.setFrequencyThreshold((int) (getParameterAsDouble(PARAMETER_FREQUENCY_THRESHOLD_KEY) * 100));
		parameters.setDistanceThreshold((int) (getParameterAsDouble(PARAMETER_DISTANCE_THRESHOLD_KEY) * 100));

		XLog logModified = highFrequencyFilterPlugin.run(getPluginContext(), logOriginal, parameters);

		XLogIOObject result = new XLogIOObject(logModified, getPluginContext());
		outputEventLog.deliver(result);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		parameterTypes.add(new ParameterTypeDouble(PARAMETER_FREQUENCY_THRESHOLD_KEY,
				PARAMETER_FREQUENCY_THRESHOLD_DESCR, 0.0, 1.0, 0.05));
		parameterTypes.add(new ParameterTypeDouble(PARAMETER_DISTANCE_THRESHOLD_KEY, PARAMETER_DISTANCE_THRESHOLD_DESCR,
				0.0, 1.0, 0.03));
		return parameterTypes;
	}

}