package org.rapidprom.operators.util;

import java.util.ArrayList;
import java.util.List;

import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.external.connectors.prom.RapidProMPluginContext;

import com.rapidminer.operator.MemoryCleanUp;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

public class RapidProMMemoryCleanUpOperator extends MemoryCleanUp {

	public RapidProMMemoryCleanUpOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		PluginContext context = RapidProMGlobalContext.instance().getPluginContext();
		List<PluginContext> children = new ArrayList<PluginContext>(context.getChildContexts());
		for(PluginContext childContext : children){
			if(childContext instanceof RapidProMPluginContext)
				((RapidProMPluginContext) childContext).closeExecutor();
			context.deleteChild(childContext);
		}
		context.clear();
		
		super.doWork();
	}

}
