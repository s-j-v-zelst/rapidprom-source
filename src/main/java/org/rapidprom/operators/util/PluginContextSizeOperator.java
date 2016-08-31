package org.rapidprom.operators.util;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.tools.LogService;

public class PluginContextSizeOperator extends AbstractRapidProMDiscoveryOperator {

	private OutputPort output = getOutputPorts().createPort("example set");

	public PluginContextSizeOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, ExampleSet.class));
	}

	public void doWork() throws OperatorException {

		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: counting unique traces");
		long time = System.currentTimeMillis();

		PluginContext pc = RapidProMGlobalContext.instance().getPluginContext();

		Object[][] outputString = new Object[2][2];
		outputString[0][0] = "Size of the Connection Manager";
		outputString[0][1] = pc.getConnectionManager().getConnectionIDs().size();
		outputString[1][0] = "Size of the Provided Object Manager";
		outputString[1][1] = pc.getProvidedObjectManager().getProvidedObjects().size();

		ExampleSet es = ExampleSetFactory.createExampleSet(outputString);

		output.deliver(es);

		logger.log(Level.INFO, "End: counting unique traces (" + (System.currentTimeMillis() - time) / 1000 + " sec)");

	}
}
