package org.rapidprom.operators.util;

import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;

import com.rapidminer.operator.MemoryCleanUp;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

public class RapidProMMemoryCleanUpOperator extends MemoryCleanUp {

	public RapidProMMemoryCleanUpOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		RapidProMGlobalContext.instance().getPluginContext().clear();
		super.doWork();
	}

}
