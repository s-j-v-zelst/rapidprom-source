package org.rapidprom.operators.logmanipulation;

import java.util.List;
import org.deckfour.xes.model.XLog;
import org.processmining.log.parameters.LowFrequencyFilterParameters;
import org.processmining.log.plugins.LowFrequencyFilterPlugin;
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
public class FilterLowFrequencyTracesOperator extends AbstractRapidProMLogManipulationOperator {

	private static final String PARAMETER_FREQUENCY_THRESHOLD_KEY = "Frequency threshold",
			PARAMETER_FREQUENCY_THRESHOLD_DESCR = "Frequency threshold. For example, 0.05 will remove the 5% least-occurring traces of the log. "
					+ "The value 0.0 includes all traces, and the value 1.0 only the most frequent trace.";
	
	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public FilterLowFrequencyTracesOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		XLog logOriginal = getXLog();
		LowFrequencyFilterPlugin lowFrequencyFilterPlugin = new LowFrequencyFilterPlugin();

		// TODO missing constructor without log
		LowFrequencyFilterParameters parameters = new LowFrequencyFilterParameters(logOriginal);
		parameters.setClassifier(getXEventClassifier());

		double threshold = getParameterAsDouble(PARAMETER_FREQUENCY_THRESHOLD_KEY);
		parameters.setThreshold((int) (threshold * 100));

		XLog logModified = lowFrequencyFilterPlugin.run(getPluginContext(), logOriginal, parameters);

		XLogIOObject result = new XLogIOObject(logModified, getPluginContext());
		outputEventLog.deliver(result);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		parameterTypes.add(new ParameterTypeDouble(PARAMETER_FREQUENCY_THRESHOLD_KEY,
				PARAMETER_FREQUENCY_THRESHOLD_DESCR, 0.0, 1.0, 0.5));
		return parameterTypes;
	}

}