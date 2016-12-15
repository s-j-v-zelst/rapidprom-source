package org.rapidprom.operators.logmanipulation;

import java.util.List;
import java.util.Set;

import org.deckfour.xes.model.XLog;
import org.processmining.log.utils.XUtils;
import org.processmining.logenhancement.transforming.MoveCommonAttributesToTracePlugin;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;
import org.rapidprom.operators.util.RapidProMProgress;

import com.google.common.collect.ImmutableSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;

/**
 * Plug-in that moves all common attributes (i.e. appear in each event with the
 * same value) to trace level. Taken from the LogEnhancement package of ProM.
 * 
 * @author F. Mannhardt
 *
 */
public class MoveCommonAttributeToTraceOperator extends AbstractRapidProMLogManipulationOperator {

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public MoveCommonAttributeToTraceOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog logOriginal = getXLog();

		// TODO user configurable
		Set<String> attributeToCheck = XUtils.getEventAttributeKeys(logOriginal);
		Set<String> attributesToMove = ImmutableSet.of();

		XLog changedLog = (XLog) logOriginal.clone();

		new MoveCommonAttributesToTracePlugin().doMoveAttributesToTrace(changedLog, attributeToCheck, attributesToMove,
				new RapidProMProgress(getProgress()));

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