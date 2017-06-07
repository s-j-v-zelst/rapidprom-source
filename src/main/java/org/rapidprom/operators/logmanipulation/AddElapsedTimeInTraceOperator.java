package org.rapidprom.operators.logmanipulation;

import java.util.List;

import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.impl.XAttributeContinuousImpl;
import org.processmining.logenhancement.enriching.AddElapsedTimePlugin;
import org.processmining.logenhancement.enriching.TimeResolution;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;
import org.rapidprom.operators.util.RapidProMProgress;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeString;

/**
 * Merges subsequent events in an event log based on several heuristics. Taken
 * from the LogEnhancement package of ProM.
 * 
 * @author F. Mannhardt
 *
 */
public class AddElapsedTimeInTraceOperator extends AbstractRapidProMLogManipulationOperator {

	private static final String PARAMETER_ATTRIBUTE_KEY = "Attribute name",
			PARAMETER_ATTRIBUTE_DESCR = "The name of the attribute that will be added.",
			PARAMETER_TIME_RESOLUTION_KEY = "Time resolution",
			PARAMETER_TIME_RESOLUTION_DESCR = "Resolution of the elapsed time.";

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public AddElapsedTimeInTraceOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	public void doWork() throws OperatorException {

		XLog logOriginal = getXLog();
		XLog logModified = (XLog) logOriginal.clone();
		
		String attributeName = getParameterAsString(PARAMETER_ATTRIBUTE_KEY);
		new AddElapsedTimePlugin().doAddElapsedTimeInTrace(new RapidProMProgress(getProgress()), logModified, attributeName, TimeResolution.valueOf(getParameterAsString(PARAMETER_TIME_RESOLUTION_KEY)));

		logModified.getGlobalEventAttributes().add(new XAttributeContinuousImpl(attributeName, -1.0d));

		XLogIOObject result = new XLogIOObject(logModified, getPluginContext());
		outputEventLog.deliver(result);
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeString parameterAttributeName = new ParameterTypeString(PARAMETER_ATTRIBUTE_KEY,
				PARAMETER_ATTRIBUTE_DESCR, "elapsedTime");
		parameterTypes.add(parameterAttributeName);

		ParameterTypeCategory parameterTime = new ParameterTypeCategory(PARAMETER_TIME_RESOLUTION_KEY,
				PARAMETER_TIME_RESOLUTION_DESCR, enumValuesToStringArray(TimeResolution.values()), 0, false);
		parameterTypes.add(parameterTime);

		return parameterTypes;
	}

}