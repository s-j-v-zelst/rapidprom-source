package org.rapidprom.operators.logmanipulation;

import java.util.List;
import java.util.Set;

import org.deckfour.xes.model.XLog;
import org.processmining.log.utils.XUtils;
import org.processmining.logenhancement.transforming.RemoveDuplicateAttributeValuesPlugin;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;
import org.rapidprom.operators.util.RapidProMProgress;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;

/**
 * Removes duplicate attribute values from events, i.e., attribute that have not
 * change since the last event occurred. This creates a smaller sparse event
 * log, which is useful to reduce the memory consumption. Taken from the
 * LogEnhancement package of ProM.
 * 
 * @author F. Mannhardt
 *
 */
public class RemoveDuplicateAttributeValuesOperator extends AbstractRapidProMLogManipulationOperator {

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public RemoveDuplicateAttributeValuesOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog logOriginal = getXLog();

		// TODO user configurable
		Set<String> relevantAttributes = XUtils.getEventAttributeKeys(logOriginal);

		XLog changedLog = RemoveDuplicateAttributeValuesPlugin.doMergeSubsequentEvents(
				new RapidProMProgress(getProgress()), (XLog) logOriginal.clone(), relevantAttributes);

		XLogIOObject result = new XLogIOObject(changedLog, getPluginContext());
		outputEventLog.deliver(result);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		parameterTypes.clear();
		return parameterTypes;
	}

}