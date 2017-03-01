package org.rapidprom.operators.logmanipulation;

import java.text.DateFormat;
import java.util.List;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.log.repair.RepairAttributeDataType;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;
import org.rapidprom.operators.util.RapidProMProgress;
import com.google.common.collect.ImmutableList;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;

/**
 * Repairs the data types of the log
 * 
 * @author F. Mannhardt
 *
 */
public class RepairLogOperator extends AbstractRapidProMLogManipulationOperator {

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public RepairLogOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog logOriginal = getXLog();
		
		PluginContext context = RapidProMGlobalContext.instance().getProgressAwarePluginContext(new RapidProMProgress(getProgress()));
		
		XLog modifiedLog = (XLog) logOriginal.clone();
		new RepairAttributeDataType().doRepairEventAttributes(context, modifiedLog, ImmutableList.<DateFormat>of());

		XLogIOObject result = new XLogIOObject(modifiedLog, getPluginContext());
		outputEventLog.deliver(result);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		parameterTypes.clear();
		return parameterTypes;
	}

}