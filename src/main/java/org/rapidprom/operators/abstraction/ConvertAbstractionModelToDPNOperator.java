package org.rapidprom.operators.abstraction;

import java.util.List;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;

/**
 * Converts the {@link AbstractionModelIOObject} to an
 * {@link DataPetriNetIOObject}.
 * 
 * @author F. Mannhardt
 *
 */
public class ConvertAbstractionModelToDPNOperator extends Operator {

	private static final String SIMPLIFY_MODEL_KEY = "Simplify Model",
			SIMPLIFY_MODEL_DESCR = "Simplify the generated Data Petri net by removing unnecessary silent transitions and simplifying unbounded repetitions.";

	private InputPort inputAbstractionModel = getInputPorts().createPort("abstraction model (ProM Abstraction Model)",
			AbstractionModelIOObject.class);
	private OutputPort output = getOutputPorts().createPort("model (ProM Data Petri Net)");

	public ConvertAbstractionModelToDPNOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, AbstractionModelIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		AbstractionModelIOObject obj = inputAbstractionModel.getData(AbstractionModelIOObject.class);
		AbstractionModel abstractionModel = obj.getArtifact();
		DataPetriNetsWithMarkings dpn = abstractionModel.getCombinedDPN();
		output.deliver(new DataPetriNetIOObject(dpn, dpn.getInitialMarking(), dpn.getFinalMarkings()[0], getContext()));

	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeBoolean(SIMPLIFY_MODEL_KEY, SIMPLIFY_MODEL_DESCR, true));
		return params;
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}