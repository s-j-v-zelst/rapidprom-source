package org.rapidprom.operators.filtering;

import java.util.List;

import org.deckfour.xes.model.XLog;
import org.processmining.noisefiltering.plugins.RProMNoiseFilterPlugin;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.filtering.abstr.AbstractFilteringOperator;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;

public class FilterOutInfrequentBehaviorLogAutomatonOperatorImpl extends AbstractFilteringOperator {

	private static final String PARAMETER_KEY_FILTER_THRESHOLD = "Threshold";
	private static final String PARAMETER_DESC_FILTER_THRESHOLD = "Set the filtering threshold which is used in pruning the Anomaly-Free Automaton";

	public FilterOutInfrequentBehaviorLogAutomatonOperatorImpl(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		XLog noisyLog = getInputLogPort().getData(XLogIOObject.class).getArtifact();
		getOutputLogPort().deliver(new XLogIOObject(
				RProMNoiseFilterPlugin.apply(noisyLog, getParameterAsDouble(PARAMETER_KEY_FILTER_THRESHOLD)),
				RapidProMGlobalContext.instance().getPluginContext()));
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeDouble(PARAMETER_KEY_FILTER_THRESHOLD, PARAMETER_DESC_FILTER_THRESHOLD, 0, 1, 0.25,
				false));
		return params;
	}

}
