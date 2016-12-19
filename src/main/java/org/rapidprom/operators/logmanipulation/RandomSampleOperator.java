package org.rapidprom.operators.logmanipulation;

import java.util.List;
import org.deckfour.xes.model.XLog;
import org.processmining.logenhancement.filtering.RandomLogSamplePlugin;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;
import org.rapidprom.operators.util.RapidProMProgress;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;

/**
 * Extracts a random sample of traces from the event log. The size of the sample
 * can be given. Taken from the LogEnhancement package of ProM.
 * 
 * @author F. Mannhardt
 *
 */
public class RandomSampleOperator extends AbstractRapidProMLogManipulationOperator {

	private static final String SAMPLE_SIZE_KEY = "Sample size",
			SAMPLE_SIZE_DESCR = "Required sample size in number of traces";

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public RandomSampleOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog logOriginal = getXLog();

		int numTraces = getParameterAsInt(SAMPLE_SIZE_KEY);

		XLog changedLog = RandomLogSamplePlugin.createRandomSampleLog(logOriginal, numTraces,
				new RapidProMProgress(getProgress()));

		XLogIOObject result = new XLogIOObject(changedLog, getPluginContext());
		outputEventLog.deliver(result);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		parameterTypes.clear();

		ParameterTypeInt sampleSizeParameter = new ParameterTypeInt(SAMPLE_SIZE_KEY, SAMPLE_SIZE_DESCR, 0,
				Integer.MAX_VALUE, 1);
		parameterTypes.add(sampleSizeParameter);

		return parameterTypes;
	}

}