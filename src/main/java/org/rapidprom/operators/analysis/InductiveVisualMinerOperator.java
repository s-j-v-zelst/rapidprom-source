package org.rapidprom.operators.analysis;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.plugins.inductiveVisualMiner.InductiveVisualMiner;
import org.processmining.plugins.inductiveVisualMiner.InductiveVisualMiner.InductiveVisualMinerLauncher;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.InductiveVisualMinerLauncherIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.tools.LogService;

public class InductiveVisualMinerOperator extends AbstractRapidProMEventLogBasedOperator {

	private OutputPort outputInteractiveMinerLauncher = getOutputPorts()
			.createPort("model (ProM InteractiveVisualMiner)");

	public InductiveVisualMinerOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(
				new GenerateNewMDRule(outputInteractiveMinerLauncher, InductiveVisualMinerLauncherIOObject.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: inductive visual miner");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();
		InductiveVisualMiner wrapper = new InductiveVisualMiner();
		InductiveVisualMinerLauncher im = wrapper.mineGuiProcessTree(pluginContext, getXLog());

		InductiveVisualMinerLauncherIOObject interactiveMinerLauncherIOObject = new InductiveVisualMinerLauncherIOObject(
				im, pluginContext);

		outputInteractiveMinerLauncher.deliver(interactiveMinerLauncherIOObject);

		logger.log(Level.INFO, "End: inductive visual miner (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

}
