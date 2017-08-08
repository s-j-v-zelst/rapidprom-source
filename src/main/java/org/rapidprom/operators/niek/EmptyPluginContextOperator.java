package org.rapidprom.operators.niek;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.tools.LogService;

public class EmptyPluginContextOperator extends Operator {

	public EmptyPluginContextOperator(OperatorDescription description) {
		super(description);
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: Empty Plugin Context");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();
		cleanupPluginContext(pluginContext.getRootContext());
		
		logger.log(Level.INFO, "End: Empty Plugin Context ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}
	
	public void cleanupPluginContext(PluginContext pluginContext){
		List<PluginContext> children = new ArrayList<PluginContext>(pluginContext.getChildContexts());
		for(PluginContext child : children){
			cleanupPluginContext(child);
			pluginContext.deleteChild(child);
		}
				
		pluginContext.getProvidedObjectManager().clear();
	
		pluginContext.getConnectionManager().clear();
	}
}