package org.rapidprom.operators.niek;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.tools.LogService;

public class AddNoisyActivitiesToLogOperator extends Operator {

	private static final String PARAMETER_0_KEY = "Number of Noisy Activities",
		PARAMETER_0_DESCR = "The number of noisy activities to insert into the event log.";

	private InputPort inputXLog = getInputPorts()
			.createPort("event log (ProM Event Log)", XLogIOObject.class);
	private OutputPort outputEventLog = getOutputPorts()
			.createPort("event log (ProM Event Log)");

	public AddNoisyActivitiesToLogOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog,
				XLogIOObject.class));
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeInt parameterType1 = new ParameterTypeInt(PARAMETER_0_KEY,
				PARAMETER_0_DESCR, 0, Integer.MAX_VALUE, 1);
		parameterTypes.add(parameterType1);

		return parameterTypes;
	}
	
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: Filter Log using Randomness");
		long time = System.currentTimeMillis();

		AddNoisyActivitiesToLog transformer = new AddNoisyActivitiesToLog();

		XLogIOObject logWrapper = inputXLog.getData(XLogIOObject.class);

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		XLogIOObject result = new XLogIOObject(transformer.transform(null, logWrapper.getArtifact(), getParameterAsInt(PARAMETER_0_KEY)), pluginContext);
		
		outputEventLog.deliver(result);

		logger.log(Level.INFO, "End: Filter Log using Randomness ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

}